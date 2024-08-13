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
    if not dag_run or not dag_run.conf:
        logger.error("No DAG run or configuration found.")
        raise KeyError("No configuration found in the DAG run.")

    # Extract and log the configuration
    config = dag_run.conf
    logger.info("DAG Run Configuration: %s", config)

    # Retrieve unique_id from the configuration
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
