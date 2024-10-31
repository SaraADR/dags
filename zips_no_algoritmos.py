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
    print(f"Valor de minio desde kwargs['dag_run'].conf: {minio}")

    if not minio:
        print("Error: No se recibió el nombre del archivo desde MinIO.")
        return

    # Establecer conexión con MinIO
    try:
        connection = BaseHook.get_connection('minio_conn')
        extra = json.loads(connection.extra)
        s3_client = boto3.client(
            's3',
            endpoint_url=extra.get('endpoint_url'),
            aws_access_key_id=extra.get('aws_access_key_id'),
            aws_secret_access_key=extra.get('aws_secret_access_key'),
            config=Config(signature_version='s3v4')
        )
        print("Conexión a MinIO establecida con éxito.")
    except Exception as e:
        print(f"Error al establecer conexión con MinIO: {e}")
        return

    # Nombre del bucket donde está almacenado el archivo/carpeta
    bucket_name = 'tmp'
    folder_prefix = 'temp/sftp/'
    local_directory = 'temp'

    try:
        local_zip_path = download_from_minio(s3_client, bucket_name, minio, local_directory, folder_prefix)
        if local_zip_path:
            print(f"Archivo descargado en la ruta local: {local_zip_path}")
            process_zip_file(local_zip_path, minio, **kwargs)
        else:
            print("Error: La descarga desde MinIO no devolvió una ruta válida.")
    except Exception as e:
        print(f"Error durante la descarga o procesamiento del archivo desde MinIO: {e}")
        raise
    return 0

def download_from_minio(s3_client, bucket_name, file_path_in_minio, local_directory, folder_prefix):
    """
    Función para descargar archivos o carpetas desde MinIO.
    """
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    local_file = os.path.join(local_directory, os.path.basename(file_path_in_minio))
    print(f"Intentando descargar desde MinIO: bucket_name={bucket_name}, file_path_in_minio={file_path_in_minio}, local_file={local_file}")
    
    try:
        # Verificar si el archivo existe antes de intentar descargarlo
        response = s3_client.get_object(Bucket=bucket_name, Key=f"{folder_prefix}{file_path_in_minio}")
        with open(local_file, 'wb') as f:
            f.write(response['Body'].read())

        print(f"Archivo descargado correctamente: {local_file}")
        return local_file
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"Error 404: El archivo no fue encontrado en MinIO: {file_path_in_minio}")
        else:
            print(f"Error al intentar descargar desde MinIO: {e}")
        return None

def process_zip_file(local_zip_path, nombre_fichero, **kwargs):
    if not local_zip_path:
        print("Error: No se pudo descargar el archivo desde MinIO.")
        return

    try:
        with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
            zip_file.testzip()
            print("El archivo ZIP es válido.")

            with tempfile.TemporaryDirectory() as temp_dir:
                print(f"Directorio temporal creado: {temp_dir}")
                zip_file.extractall(temp_dir)

                file_list = zip_file.namelist()
                print("Archivos en el ZIP:", file_list)
    except zipfile.BadZipFile as e:
        print(f"Error: El archivo no es un ZIP válido: {e}")
    except Exception as e:
        print(f"Error al procesar el archivo ZIP: {e}")

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
    description='DAG que lee todo lo que sea un zip pero no un algoritmo',
    catchup=False,
)

process_extracted_files_task = PythonOperator(
    task_id='process_extracted_files',
    python_callable=process_extracted_files,
    provide_context=True,
    dag=dag,
)

process_extracted_files_task
