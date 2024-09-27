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
import glob

def find_and_modify_files(temp_dir='/scripts'):
    try:
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
        config_json = os.path.join(temp_dir, 'share_data/input/config.json')
        config_env = os.path.join(temp_dir, 'launch/.env')
        config_automaps = os.path.join(temp_dir, 'launch/automaps.tar')
        config_compose = os.path.join(temp_dir, 'launch/compose.yaml')
        config_run = os.path.join(temp_dir, 'launch/run.sh')

        # Crear las carpetas necesarias
        os.makedirs(os.path.dirname(config_json), exist_ok=True)
        os.makedirs(os.path.dirname(config_env), exist_ok=True)
        os.makedirs(os.path.dirname(config_automaps), exist_ok=True)
        os.makedirs(os.path.dirname(config_compose), exist_ok=True)
        os.makedirs(os.path.dirname(config_run), exist_ok=True)

        # Descargar archivos de MinIO
        s3_client.download_file(bucket_name, 'share_data/input/config.json', config_json)
        s3_client.download_file(bucket_name, 'launch/.env', config_env)
        s3_client.download_file(bucket_name, 'launch/automaps.tar', config_automaps)
        s3_client.download_file(bucket_name, 'launch/compose.yaml', config_compose)
        s3_client.download_file(bucket_name, 'launch/run.sh', config_run)

        # Chequear archivos descargados
        downloaded_files = glob.glob(os.path.join(temp_dir, '**/*'), recursive=True)
        print("Archivos descargados:", downloaded_files)

    except Exception as e:
        print(f"Error: {str(e)}")
        return

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
    empty_dir=k8s.V1EmptyDirVolumeSource()
)

empty_dir_volume_mount = k8s.V1VolumeMount(
    name='empty-dir-volume',
    mount_path='/scripts'
)

security_context = k8s.V1SecurityContext(
    privileged=True
)

# Tarea que ejecuta el script run.sh usando Docker-in-Docker
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
    security_context=security_context,
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

# Definir la secuencia de tareas
find_and_modify_files_task >> run_with_docker_task
