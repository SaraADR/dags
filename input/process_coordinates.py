from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import boto3
import os

# Definir parámetros del DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Crear DAG
dag = DAG(
    'process_coordinates',
    default_args=default_args,
    description='DAG to process coordinates, convert to PDF and upload to MinIO',
    schedule_interval=None,
)

def upload_to_minio(output_file, bucket_name, object_name):
    minio_url = 'http://minio:9000'
    access_key = 'minioadmin'
    secret_key = 'minioadmin'

    minio_client = boto3.client('s3', endpoint_url=minio_url, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    minio_client.upload_file(output_file, bucket_name, object_name)

def upload_to_minio_task(**kwargs):
    output_file = '/output/process_coordinates.pdf'
    bucket_name = 'my-bucket'
    object_name = 'process_coordinates.pdf'
    upload_to_minio(output_file, bucket_name, object_name)

# Definir tareas
convert_to_pdf = BashOperator(
    task_id='convert_to_pdf',
    bash_command='docker-compose run --rm pdf_converter /input/process_coordinates.py /output/process_coordinates.pdf',
    dag=dag,
)

upload_pdf_to_minio = PythonOperator(
    task_id='upload_pdf_to_minio',
    python_callable=upload_to_minio_task,
    dag=dag,
)

# Definir el flujo de tareas
convert_to_pdf >> upload_pdf_to_minio
