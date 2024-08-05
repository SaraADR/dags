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

def retrieve_from_minio(**context):
    try:
        message = context['dag_run'].conf
        if not message:
            raise KeyError("No message found in the DAG run configuration.")
    except KeyError:
        raise KeyError("No configuration found in the DAG run configuration.")
    
    try:
        unique_id = message.get('unique_id', None)
        if not unique_id:
            raise ValueError("No valid 'unique_id' key found in the DAG run configuration.")
    except KeyError:
        raise ValueError("No valid 'unique_id' key found in the DAG run configuration.")

    # Realiza la búsqueda y recuperación del archivo en MinIO
    file_content = fetch_from_minio(unique_id)
    
    if file_content:
        # Puedes procesar el archivo recuperado o simplemente devolverlo como respuesta
        print(f"Archivo con ID único {unique_id} recuperado de MinIO.")
    else:
        raise FileNotFoundError(f"No se encontró el archivo con ID único {unique_id} en MinIO.")

def fetch_from_minio(unique_id):
    # Obtener la conexión de MinIO desde Airflow
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)

    # Crear el cliente de MinIO/S3 con las credenciales y configuración necesarias
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

    bucket_name = 'locationtest'
    
    # Listar los objetos en el bucket y buscar el que tenga el tag con el unique_id
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    for obj in response.get('Contents', []):
        key = obj['Key']
        obj_tags = s3_client.get_object_tagging(Bucket=bucket_name, Key=key)
        for tag in obj_tags.get('TagSet', []):
            if tag['Key'] == 'unique_id' and tag['Value'] == unique_id:
                # Descargar el archivo si se encuentra
                file_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
                return file_obj['Body'].read()
    
    return None

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
