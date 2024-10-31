import base64
import json
import tempfile
import uuid
import zipfile
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3
from botocore.client import Config
from airflow.hooks.base_hook import BaseHook
import os
from botocore.exceptions import ClientError

# Function to process extracted files and retrieve metadata
def process_extracted_files(**kwargs):
    minio = kwargs['dag_run'].conf.get('minio')
    print(f"Mensaje: {minio}")

    if not minio:
        print("Ha habido un error con el traspaso de los documentos")
        return

    # Establish connection with MinIO
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

    bucket_name = 'tmp'
    folder_prefix = 'temp/sftp/'
    local_directory = 'temp'  # Path for local storage

    try:
        # Download ZIP file from MinIO
        local_zip_path = download_from_minio(s3_client, bucket_name, minio, local_directory, folder_prefix)
        print(f"ZIP file path: {local_zip_path}")
        # Process the ZIP file for metadata extraction
        extract_metadata_from_zip(local_zip_path, **kwargs)
    except Exception as e:
        print(f"Error al descargar desde MinIO: {e}")
        raise

    return 0

# Helper function to download file from MinIO
def download_from_minio(s3_client, bucket_name, file_path_in_minio, local_directory, folder_prefix):
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    local_file = os.path.join(local_directory, os.path.basename(file_path_in_minio))
    print(f"Downloading file from MinIO: {file_path_in_minio} to {local_file}")

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_path_in_minio)
        with open(local_file, 'wb') as f:
            f.write(response['Body'].read())

        print(f"File downloaded successfully: {local_file}")
        return local_file
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Error 404: File not found in MinIO: {file_path_in_minio}")
        else:
            print(f"Error in process: {str(e)}")
        return None

# Function to extract metadata from files in the ZIP
def extract_metadata_from_zip(local_zip_path, **kwargs):
    if local_zip_path is None:
        print("ZIP file path is None, cannot proceed.")
        return

    try:
        with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
            zip_file.testzip()  # Ensure ZIP integrity
            print("ZIP file is valid.")

            with tempfile.TemporaryDirectory() as temp_dir:
                print(f"Temporary directory created: {temp_dir}")

                # Extract and process files for metadata
                zip_file.extractall(temp_dir)
                file_list = zip_file.namelist()

                metadata = []
                for file_name in file_list:
                    file_path = os.path.join(temp_dir, file_name)
                    if os.path.isfile(file_path):
                        # Gather metadata
                        file_metadata = {
                            "file_name": file_name,
                            "file_size": os.path.getsize(file_path),
                            "modification_time": datetime.fromtimestamp(os.path.getmtime(file_path))
                        }
                        metadata.append(file_metadata)
                        print(f"Metadata for {file_name}: {file_metadata}")

                # Optionally, log metadata or push it to an external system
                print("Extracted metadata:", json.dumps(metadata, default=str))
    except zipfile.BadZipFile:
        print("Invalid ZIP file detected.")
    except Exception as e:
        print(f"Error processing ZIP file: {e}")

# Define default arguments for the DAG
default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Initialize DAG
dag = DAG(
    'zip_metadata_extractor',
    default_args=default_args,
    description='DAG that extracts metadata from files within a ZIP in MinIO',
    schedule_interval=None,
    catchup=False,
)

# Define the task
process_extracted_files_task = PythonOperator(
    task_id='process_extracted_files',
    python_callable=process_extracted_files,
    provide_context=True,
    dag=dag,
)
