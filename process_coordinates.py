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
    'kafka_to_minio_pipeline',
    default_args=default_args,
    description='Pipeline to process Kafka messages, convert to PDF and upload to MinIO',
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

# Tarea para ejecutar kafka_consumer_archivos.py
run_kafka_consumer = BashOperator(
    task_id='run_kafka_consumer',
    bash_command='python /path/to/kafka_consumer_archivos.py',
    dag=dag,
)

# Tarea para convertir el archivo a PDF
convert_to_pdf = BashOperator(
    task_id='convert
