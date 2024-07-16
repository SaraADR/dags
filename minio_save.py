import io
import json
import tempfile
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import zipfile
import ast
import boto3
from botocore.client import Config
from airflow.hooks.base_hook import BaseHook


def save_to_minio(file_name, content):
    # Obtener la conexión de MinIO desde Airflow
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )


    bucket_name = 'avincis-test'  


    # Crear el bucket si no existe
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except s3_client.exceptions.NoSuchBucket:
        s3_client.create_bucket(Bucket=bucket_name)

    # Subir el archivo a MinIO
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=io.BytesIO(content)
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
    'save_documents_to_minio',
    default_args=default_args,
    description='A simple DAG to save documents to MinIO',
    schedule_interval=timedelta(days=1),
)
minio_task = PythonOperator(
    task_id='minio_task',
    python_callable=save_to_minio,
    provide_context=True,
    dag=dag,
)


minio_task
