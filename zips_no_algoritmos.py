import base64
import json
import tempfile
import uuid
import zipfile
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

def process_extracted_files(**kwargs):
    minio = kwargs['dag_run'].conf.get('minio')
    print(f"Mensaje: {minio}")

    if not minio:
        print("Error: No se encontró el archivo en MinIO")
        return

    # Establecer conexión con MinIO
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

    # Nombre del bucket y prefijo de carpeta
    bucket_name = 'imagestiffs'  # Bucket nuevo
    

    # Descargar archivo desde MinIO
    local_directory = 'temp'  
    try:
        local_zip_path = download_from_minio(s3_client, bucket_name, minio, local_directory)
        if local_zip_path:
            print(f"Archivo descargado correctamente: {local_zip_path}")
            process_zip_file(local_zip_path, minio, **kwargs)
        else:
            print("Error al descargar el archivo desde MinIO.")
    except Exception as e:
        print(f"Error al procesar el archivo desde MinIO: {e}")
        raise
    return 0

def download_from_minio(s3_client, bucket_name, file_path_in_minio, local_directory):
    """
    Función para descargar archivos o carpetas desde MinIO.
    """
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    local_file = os.path.join(local_directory, os.path.basename(file_path_in_minio))
    print(f"Descargando archivo desde MinIO: {file_path_in_minio} a {local_file}")
    
    try:
        # Verificar si el archivo existe antes de descargar
        response = s3_client.get_object(Bucket=bucket_name, Key=file_path_in_minio)
        with open(local_file, 'wb') as f:
            f.write(response['Body'].read())
        print(f"Archivo descargado correctamente: {local_file}")
        return local_file
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Error 404: El archivo no se encontró en MinIO: {file_path_in_minio}")
        else:
            print(f"Error en el proceso de descarga: {str(e)}")
        return None

def process_zip_file(local_zip_path, nombre_fichero, **kwargs):
    if local_zip_path is None:
        print("Error: No se pudo descargar el archivo desde MinIO.")
        return

    try:
        if not os.path.exists(local_zip_path):
            print(f"Archivo no encontrado: {local_zip_path}")
            return

        # Validar y extraer archivo ZIP
        with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
            zip_file.testzip()
            print("El archivo ZIP es válido.")
            with tempfile.TemporaryDirectory() as temp_dir:
                print(f"Directorio temporal creado: {temp_dir}")
                zip_file.extractall(temp_dir)
                file_list = zip_file.namelist()
                print("Archivos en el ZIP:", file_list)

    except zipfile.BadZipFile:
        print("El archivo ZIP no es válido.")
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
    description='DAG que lee archivos ZIP desde MinIO',
    catchup=False,
)

process_extracted_files_task = PythonOperator(
    task_id='process_extracted_files',
    python_callable=process_extracted_files,
    provide_context=True,
    dag=dag,
)
