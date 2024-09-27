import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.hooks.base_hook import BaseHook
import boto3
import os
import tempfile
import json
from botocore.client import Config
from kubernetes.client import models as k8s

# Función para descargar y modificar los archivos
def find_and_modify_files():
    temp_dir = '/scripts'

    try:
        # Obtener conexión MinIO desde Airflow
        connection = BaseHook.get_connection('minio_conn')
        extra = json.loads(connection.extra)
        s3_client = boto3.client(
            's3',
            endpoint_url=extra['endpoint_url'],
            aws_access_key_id=extra['aws_access_key_id'],
            aws_secret_access_key=extra['aws_secret_access_key'],
            config=Config(signature_version='s3v4')
        )

        bucket_name = 'algorithms'

        # Definir los objetos y sus rutas locales
        object_key_config = 'share_data/input/config.json'
        config_json = os.path.join(temp_dir, 'share_data/input/config.json')

        object_key_env = 'launch/.env'
        config_env = os.path.join(temp_dir, 'launch/.env')
        object_key_automaps = 'launch/automaps.tar'
        config_automaps = os.path.join(temp_dir, 'launch/automaps.tar')
        object_key_compose = 'launch/compose.yaml'
        config_compose = os.path.join(temp_dir, 'launch/compose.yaml')
        object_key_run = 'launch/run.sh'
        config_run = os.path.join(temp_dir, 'launch/run.sh')

        # Crear las carpetas necesarias
        os.makedirs(os.path.dirname(config_json), exist_ok=True)
        os.makedirs(os.path.dirname(config_env), exist_ok=True)
        os.makedirs(os.path.dirname(config_automaps), exist_ok=True)
        os.makedirs(os.path.dirname(config_compose), exist_ok=True)
        os.makedirs(os.path.dirname(config_run), exist_ok=True)

        # Descargar archivos de MinIO
        s3_client.download_file(bucket_name, object_key_config, config_json)
        s3_client.download_file(bucket_name, object_key_env, config_env)
        s3_client.download_file(bucket_name, object_key_automaps, config_automaps)
        s3_client.download_file(bucket_name, object_key_compose, config_compose)
        s3_client.download_file(bucket_name, object_key_run, config_run)

        # Modificar el archivo .env si es necesario (ejemplo)
        with open(config_env, 'a') as env_file:
            env_file.write("\nNEW_ENV_VAR=value")

        return temp_dir

    except Exception as e:
        print(f"Error: {str(e)}")
        return

    finally:
        pass


# Argumentos del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    'dag_docker_in_kubernetes_with_emptyDir',
    default_args=default_args,
    description='Ejecuta Docker dentro de Kubernetes usando Docker-in-Docker y emptyDir',
    schedule_interval=None,
    catchup=False
)

# Tarea que descarga y modifica los archivos de MinIO
find_and_modify_files_task = PythonOperator(
    task_id='download_and_modify_files',
    python_callable=find_and_modify_files,
    dag=dag,
)

# Definir el volumen emptyDir y el montaje correctamente
empty_dir_volume = k8s.V1Volume(
    name='empty-dir-volume',
    empty_dir=k8s.V1EmptyDirVolumeSource()  # Volumen temporal emptyDir
)

empty_dir_volume_mount = k8s.V1VolumeMount(
    name='empty-dir-volume',
    mount_path='/scripts'  # Montar el volumen en el contenedor
)

security_context = k8s.V1SecurityContext(
    privileged=True  # Establecer el contenedor como privilegiado
)


# Tarea que ejecuta el script run.sh usando Docker-in-Docker
run_with_docker_task = KubernetesPodOperator(
    namespace='default',
    image="docker:20.10.7-dind",  # Imagen de Docker-in-Docker
    cmds=["/bin/sh", "-c", "/scripts/run.sh"],  # Cambiado a sh
    name="run_with_docker",
    task_id="run_with_docker_task",
    volumes=[empty_dir_volume],  # Utilizar la clase V1Volume
    volume_mounts=[empty_dir_volume_mount],  # Utilizar la clase V1VolumeMount
    env_vars={
        'DOCKER_HOST': 'tcp://localhost:2375',  # Necesario para DinD
        'DOCKER_TLS_CERTDIR': ''  # Desactiva TLS en DinD
    },
    security_context=security_context,  # Asignar el contexto de seguridad al contenedor
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

# Definir la secuencia de tareas
find_and_modify_files_task >> run_with_docker_task
