import io
import json
import tempfile
import uuid
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import boto3
from botocore.client import Config
from airflow.hooks.base_hook import BaseHook
import docker
from docker.errors import DockerException

def process_kafka_message(**context):
    try:
        message = context['dag_run'].conf
        if not message:
            raise KeyError("No message found in the DAG run configuration.")
    except KeyError:
        raise KeyError("No configuration found in the DAG run configuration.")
    
    try:
        message_content = message.get('message', None)
        if not message_content:
            raise ValueError("No valid 'message' key found in the DAG run configuration.")
    except KeyError:
        raise ValueError("No valid 'message' key found in the DAG run configuration.")

    unique_id = str(uuid.uuid4())

    if isinstance(message_content, dict):
        message_content = json.dumps(message_content)
    elif not isinstance(message_content, str):
        raise ValueError("The message content must be a string or a dictionary.")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_json_path = os.path.join(temp_dir, 'file.json')
        temp_pdf_path = os.path.join(temp_dir, 'file.pdf')

        with open(temp_json_path, 'w') as temp_json_file:
            temp_json_file.write(message_content)
        
        print(f"JSON temporal creado en {temp_json_path}")

        try:
            client = docker.from_env()
            container = client.containers.run(
                image='json-to-pdf-transformer',
                volumes={temp_dir: {'bind': '/data', 'mode': 'rw'}},
                command=f"transform-json-to-pdf /data/file.json /data/file.pdf",
                detach=True
            )
            container.wait()
            container_logs = container.logs().decode('utf-8')
            print(container_logs)
            container.remove()
        except DockerException as e:
            raise RuntimeError(f"Error while running Docker container: {e}")
        except Exception as e:
            raise RuntimeError(f"General error: {e}")

        print(f"PDF temporal creado en {temp_pdf_path}")

        save_to_minio(temp_pdf_path, unique_id)

        print(f"PDF subido a MinIO con ID único {unique_id}")

def save_to_minio(file_path, unique_id):
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)

    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

    bucket_name = 'locationtest'
    file_name = os.path.basename(file_path)

    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except s3_client.exceptions.NoSuchBucket:
        s3_client.create_bucket(Bucket=bucket_name)

    with open(file_path, 'rb') as file:
        file_content = file.read()

    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=file_content,
        Tagging=f"unique_id={unique_id}"
    )
    print(f'{file_name} subido correctamente a MinIO.')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'save_coordinates_to_minio',
    default_args=default_args,
    description='Un DAG para almacenar coordenadas en Minio',
    schedule_interval=timedelta(days=1),
)

save_task = PythonOperator(
    task_id='process_coordinates',
    provide_context=True,
    python_callable=process_kafka_message,
    dag=dag,
)

save_task
