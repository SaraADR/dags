import datetime
import os
from airflow import DAG
import tempfile
from airflow.hooks.base_hook import BaseHook
import json
import boto3
from botocore.client import Config, ClientError
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def find_the_folder():
    # Crear un directorio temporal
    temp_dir = tempfile.mkdtemp()

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

        print(f'Directorio temporal creado en: {temp_dir}')
        
        return temp_dir

    except Exception as e:
        print(f"Error: {str(e)}")
        return

    finally:
        # Limpieza del directorio temporal si es necesario
        pass
    # try:
    #     # Modificar el archivo JSON
    #     with open(config_json, 'r') as f:
    #         config_data = json.load(f)
    #         print(config_data)

    #     with open(config_json, 'w') as f:
    #         json.dump(config_data, f, indent=4)

    #     # Subir el archivo modificado a MinIO
    #     new_object_key = 'share_data/input/config_modified.json'
    #     s3_client.upload_file(config_json, bucket_name, new_object_key)
    #     print(f"Archivo modificado subido a MinIO: {new_object_key}")

    # except Exception as e:
    #     print(f"Error al modificar o subir el archivo: {str(e)}")









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
    task_id='change_state_job',
    python_callable=find_the_folder,
    dag=dag,
)

# Tarea para ejecutar el script run.sh
execute_docker_task = BashOperator(
    task_id='execute_docker',
    bash_command="bash {{ ti.xcom_pull(task_ids='find_the_folder') }}/launch/run.sh",
    dag=dag,
)


find_the_folder_task >> execute_docker_task