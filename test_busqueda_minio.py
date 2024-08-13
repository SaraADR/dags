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

import logging
from airflow.hooks.base_hook import BaseHook
import json
import boto3
from botocore.client import Config

# Configure logging
logger = logging.getLogger("airflow.task")

def retrieve_from_minio(**kwargs):
    """
    Retrieves a file from MinIO based on a unique ID provided in the DAG run configuration.
    """
    # Access the DAG run context
    dag_run = kwargs.get('dag_run')
    if not dag_run:
        logger.error("No DAG run context found.")
        raise KeyError("No DAG run context found.")

    config = dag_run.conf if dag_run.conf else {}
    logger.info("DAG Run Configuration: %s", config)

    unique_id = config.get('unique_id')
    if not unique_id:
        logger.error("No valid 'unique_id' key found in the DAG run configuration.")
        raise ValueError("No valid 'unique_id' key found in the DAG run configuration.")

    # Retrieve the file from MinIO
    file_content = fetch_from_minio(unique_id)
    if file_content:
        logger.info(f"File with unique ID {unique_id} successfully retrieved from MinIO.")
    else:
        logger.error(f"File with unique ID {unique_id} not found in MinIO.")
        raise FileNotFoundError(f"No file found in MinIO with unique ID {unique_id}.")


def fetch_from_minio(unique_id):
    """
    Fetches a file from MinIO by unique_id.
    """
    # Get MinIO connection from Airflow
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)

    # Create the S3 client
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

    # Define the bucket name
    bucket_name = 'locationtest'
    # List objects in the bucket and find the one with the matching unique_id tag
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    for obj in response.get('Contents', []):
        key = obj['Key']
        obj_tags = s3_client.get_object_tagging(Bucket=bucket_name, Key=key)
        for tag in obj_tags.get('TagSet', []):
            if tag['Key'] == 'unique_id' and tag['Value'] == unique_id:
                # Download the file if found
                file_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
                return file_obj['Body'].read()

    return None

# Ensure to add this function in the appropriate PythonOperator in your Airflow DAG.


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'schedule_interval': 1000,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'retrieve_file_from_minio',
    default_args=default_args,
    description='Un DAG para recuperar archivos de Minio basados en un ID único',
    schedule_interval=timedelta(days=1),
)

retrieve_task = PythonOperator(
    task_id='retrieve_file',
    provide_context=True,
    python_callable=retrieve_from_minio,
    dag=dag,
)

retrieve_task
