import datetime
from airflow import DAG
import tempfile
from airflow.hooks.base_hook import BaseHook
import json
import boto3
from botocore.client import Config, ClientError

def find_the_folder():
    temp_dir = tempfile.mkdtemp()
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
        object_key = 'share_data/input/config.json'
        local_file_path = f"{temp_dir}/config.json"

        s3_client.download_file(bucket_name, object_key, local_file_path)
        print(f'{temp_dir}')
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return

    try:
        # Modificar el archivo JSON
        with open(local_file_path, 'r') as f:
            config_data = json.load(f)

            print(config_data)

        with open(local_file_path, 'w') as f:
                json.dump(config_data, f, indent=4)
        
        new_object_key = 'share_data/output/config_modified.json' 
        s3_client.upload_file(local_file_path, bucket_name, new_object_key)
        print(f"Archivo modificado subido a MinIO: {new_object_key}")

    except Exception as e:
        print(f"Error: {str(e)}")

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
    task_id='change_state_job',
    python_callable=find_the_folder,
    provide_context=True,
    dag=dag,
)


find_the_folder_task