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
    temp_dir = '/tmp'
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
        
        # # Definir los objetos y sus rutas locales
        # object_key_config = 'share_data/input/config.json'
        # config_json = os.path.join(temp_dir, 'share_data/input/config.json')

        # object_key_env = 'launch/.env'
        # config_env = os.path.join(temp_dir, 'launch/.env')
        # object_key_automaps = 'launch/automaps.tar'
        # config_automaps = os.path.join(temp_dir, 'launch/automaps.tar')
        # object_key_compose = 'launch/compose.yaml'
        # config_compose = os.path.join(temp_dir, 'launch/compose.yaml')
        # object_key_run = 'launch/run.sh'
        # config_run = os.path.join(temp_dir, 'launch/run.sh')

         # Define the objects and their local paths
        files_to_download = {
            'share_data/input/config.json': os.path.join(temp_dir, 'share_data/input/config.json'),
            'launch/.env': os.path.join(temp_dir, 'launch/.env'),
            'launch/automaps.tar': os.path.join(temp_dir, 'launch/automaps.tar'),
            'launch/compose.yaml': os.path.join(temp_dir, 'launch/compose.yaml'),
            'launch/run.sh': os.path.join(temp_dir, 'launch/run.sh'),
        }
        # Create necessary directories
        for local_path in files_to_download.values():
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # # Crear las carpetas necesarias
        # os.makedirs(os.path.dirname(config_json), exist_ok=True)
        # os.makedirs(os.path.dirname(config_env), exist_ok=True)
        # os.makedirs(os.path.dirname(config_automaps), exist_ok=True)
        # os.makedirs(os.path.dirname(config_compose), exist_ok=True)
        # os.makedirs(os.path.dirname(config_run), exist_ok=True)

        # # Descargar archivos de MinIO
        # s3_client.download_file(bucket_name, object_key_config, config_json)
        # s3_client.download_file(bucket_name, object_key_env, config_env)
        # s3_client.download_file(bucket_name, object_key_automaps, config_automaps)
        # s3_client.download_file(bucket_name, object_key_compose, config_compose)
        # s3_client.download_file(bucket_name, object_key_run, config_run)

                # Download files from MinIO
        for object_key, local_path in files_to_download.items():
            s3_client.download_file(bucket_name, object_key, local_path)
            # Verify that the file was downloaded
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"File not found after download: {local_path}")


        # # Verificar que los archivos existan
        # if not os.path.exists(config_env) or not os.path.exists(config_automaps) or not os.path.exists(config_run):
        #     raise FileNotFoundError("Algunos archivos no se descargaron correctamente.")



        print(f'Directorio temporal creado en: {temp_dir}')
        return temp_dir

    except Exception as e:
        print(f"Error: {str(e)}")
        return

    finally:
        # Limpieza del directorio temporal si es necesario
        pass



def list_files_in_tmp():
    print("Listing files in /tmp:")
    for root, dirs, files in os.walk('/tmp'):
        level = root.replace('/tmp', '').count(os.sep)
        indent = ' ' * 4 * (level)
        print(f"{indent}{os.path.basename(root)}/")
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print(f"{subindent}{f}")

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
    'dag_prueba_docker2',
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

# Task to list files in /tmp
list_files_task = PythonOperator(
    task_id='list_files_in_tmp',
    python_callable=list_files_in_tmp,
    dag=dag,
)

# Task to run the Docker container
run_docker_task = BashOperator(
    task_id='run_docker',
    bash_command="""
    #!/bin/bash
    source /tmp/launch/.env

    # Check if image launch-automap_service:latest exists
    if [[ "$(docker images -q launch-automap_service:latest 2> /dev/null)" == "" ]]; then
        echo "Image launch-automap_service:latest does not exist. Loading image..."
        docker image load -i /tmp/launch/automaps.tar
    fi

    # Generate a unique container name
    container_name=${CONTAINER_NAME}

    # Run the container with the generated name
    docker-compose -f /tmp/launch/compose.yaml run --rm --name "$container_name" automap_service
    """,
    dag=dag,
)


find_the_folder_task >> list_files_task >> run_docker_task


