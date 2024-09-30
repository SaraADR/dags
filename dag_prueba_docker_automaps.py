import datetime
import os
import shutil
from airflow import DAG
import tempfile
from airflow.hooks.base_hook import BaseHook
import json
import boto3
from botocore.client import Config, ClientError
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


def find_the_folder():
    # Crear un directorio temporal
    temp_dir = '/scripts'
    os.makedirs(temp_dir, exist_ok=True)

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

        # Verificar que los archivos existan
        if not os.path.exists(config_env) or not os.path.exists(config_automaps) or not os.path.exists(config_run):
            raise FileNotFoundError("Algunos archivos no se descargaron correctamente.")



        print(f'Directorio temporal creado en: {temp_dir}')
        return temp_dir

    except Exception as e:
        print(f"Error: {str(e)}")
        return

    finally:
        # Limpieza del directorio temporal si es necesario
        pass



default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

dag = DAG(
    'dag_prueba_docker',
    default_args=default_args,
    description='Algoritmo dag_prueba_docker',
    schedule_interval=None,
    catchup=False
)


#Cambia estado de job
find_the_folder_task = PythonOperator(
    task_id='ejecutar_run',
    python_callable=find_the_folder,
    dag=dag,
)

# Definir el volumen emptyDir y el montaje correctamente
empty_dir_volume = k8s.V1Volume(
    name='empty-dir-volume',
    empty_dir=k8s.V1EmptyDirVolumeSource()
)

empty_dir_volume_mount = k8s.V1VolumeMount(
    name='empty-dir-volume',
    mount_path='/scripts'
)

security_context = k8s.V1SecurityContext(
    privileged=True
)


# Define the Init Container to fix permissions on the /scripts directory
init_container = k8s.V1Container(
    name="init-fix-permissions",
    image="busybox",
    command=["/bin/sh", "-c", "chmod -R 777 /scripts && ls -ld /scripts"],  # Adjust permissions
    volume_mounts=[k8s.V1VolumeMount(
        name='empty-dir-volume',
        mount_path='/scripts'
    )],
)

list_permissions_task = KubernetesPodOperator(
    namespace='default',
    image="busybox",
    cmds=["/bin/sh", "-c", "ls -la /scripts/share_data && ls -la /scripts/launch"],
    name="list_permissions",
    task_id="list_permissions_task",
    volumes=[empty_dir_volume],
    volume_mounts=[empty_dir_volume_mount],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

# Updated KubernetesPodOperator with security context
run_with_docker_task = KubernetesPodOperator(
    namespace='default',
    image="docker:20.10.7-dind",
    cmds=["/bin/sh", "-c", "/scripts/launch/run.sh"],
    name="run_with_docker",
    task_id="run_with_docker_task",
    volumes=[empty_dir_volume],
    volume_mounts=[empty_dir_volume_mount],
    env_vars={
        'DOCKER_HOST': 'tcp://localhost:2375',
        'DOCKER_TLS_CERTDIR': ''
    },
    security_context=k8s.V1SecurityContext(
        run_as_user=0,  # Use root for permission issues
        run_as_group=0,
        privileged=True
    ),
    get_logs=True,
    is_delete_operator_pod=True,
    init_containers=[init_container],  # Add init container here
    dag=dag,
)

find_the_folder_task >> list_permissions_task >> run_with_docker_task