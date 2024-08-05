from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3
from botocore.config import Config
import json
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'search_on_minio',
    default_args=default_args,
    description='A DAG to fetch images and metadata from MinIO',
    schedule_interval=None,  # This DAG will be triggered manually
)

def fetch_from_minio(file_name):
    # Get the MinIO connection from Airflow
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

    # Fetch the object from MinIO
    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    content = response['Body'].read()

    # Fetch the tags (metadata) of the object
    tags_response = s3_client.get_object_tagging(Bucket=bucket_name, Key=file_name)
    tags = {tag['Key']: tag['Value'] for tag in tags_response['TagSet']}
    
    return content, tags

def fetch_and_save(**kwargs):
    file_name = kwargs['file_name']
    
    # Fetch file and metadata from MinIO
    content, tags = fetch_from_minio(file_name)
    
    # Save fetched file in Airflow's temp directory
    temp_dir = '/tmp/airflow/'
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    
    file_path = os.path.join(temp_dir, file_name)
    with open(file_path, 'wb') as f:
        f.write(content)
    
    # Save metadata in a JSON file
    meta_file_path = os.path.join(temp_dir, f"{file_name}_metadata.json")
    with open(meta_file_path, 'w') as f:
        json.dump(tags, f)
    
    print(f'{file_name} and its metadata saved successfully in {temp_dir}.')

# Define the task
fetch_image_task = PythonOperator(
    task_id='fetch_image',
    python_callable=fetch_and_save,
    op_kwargs={'file_name': 'nombre_de_la_imagen.jpg'},  # Replace with your file name
    dag=dag,
)

fetch_image_task
