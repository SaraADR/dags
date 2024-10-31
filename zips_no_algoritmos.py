import base64
import json
import tempfile
import uuid
import zipfile
import logging  # Import logging for detailed logs
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
import boto3
from botocore.client import Config
from airflow.hooks.base_hook import BaseHook
import io
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.orm import sessionmaker
import os
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)

def process_extracted_files(**kwargs):
    logging.info("Starting task: process_extracted_files")
    
    minio = kwargs['dag_run'].conf.get('minio')
    logging.info(f"Received MinIO filename: {minio}")

    if not minio:
        logging.error("No MinIO file found in DAG run configuration.")
        return

    # Establish connection with MinIO
    logging.info("Setting up connection with MinIO")
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

    # Define bucket name and local download directory
    bucket_name = 'imagestiffs'
    logging.info(f"Bucket name: {bucket_name}")

    # Download file from MinIO
    try:
        logging.info(f"Attempting to download file from MinIO: {minio}")
        local_zip_path = download_from_minio(s3_client, bucket_name, minio)
        
        if local_zip_path:
            logging.info(f"File downloaded successfully: {local_zip_path}")
            process_zip_file(local_zip_path, minio, **kwargs)
        else:
            logging.error("Failed to download file from MinIO.")
    except Exception as e:
        logging.error(f"Error processing file from MinIO: {e}")
        raise
    logging.info("Finished task: process_extracted_files")
    return 0

def download_from_minio(s3_client, bucket_name, file_path_in_minio, local_directory):
    logging.info(f"Download initiated for file: {file_path_in_minio} from bucket: {bucket_name}")

    if not os.path.exists(local_directory):
        os.makedirs(local_directory)
        logging.info(f"Created local directory: {local_directory}")

    local_file = os.path.join(local_directory, os.path.basename(file_path_in_minio))
    logging.info(f"Local file path: {local_file}")
    
    try:
        # Check if the file exists in MinIO before downloading
        response = s3_client.get_object(Bucket=bucket_name, Key=file_path_in_minio)
        with open(local_file, 'wb') as f:
            f.write(response['Body'].read())
        logging.info(f"File downloaded to: {local_file}")
        return local_file
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logging.error(f"Error 404: File not found in MinIO: {file_path_in_minio}")
        else:
            logging.error(f"Download error: {str(e)}")
        return None

def process_zip_file(local_zip_path, nombre_fichero, **kwargs):
    logging.info(f"Initiating ZIP file processing: {local_zip_path}")

    if local_zip_path is None:
        logging.error("File could not be downloaded from MinIO.")
        return

    try:
        if not os.path.exists(local_zip_path):
            logging.error(f"File not found: {local_zip_path}")
            return

        # Validate and extract ZIP file
        with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
            zip_file.testzip()
            logging.info("ZIP file is valid.")
            with tempfile.TemporaryDirectory() as temp_dir:
                logging.info(f"Temporary directory created: {temp_dir}")
                zip_file.extractall(temp_dir)
                file_list = zip_file.namelist()
                logging.info(f"Files extracted from ZIP: {file_list}")

    except zipfile.BadZipFile:
        logging.error("Invalid ZIP file.")
        return

default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'zip_no_algoritmos',
    default_args=default_args,
    description='DAG that reads ZIP files from MinIO',
    catchup=False,
)

process_extracted_files_task = PythonOperator(
    task_id='process_extracted_files',
    python_callable=process_extracted_files,
    provide_context=True,
    dag=dag,
)
