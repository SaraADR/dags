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

from dag_utils import get_minio_client

# Función para eliminar archivos antiguos en el bucket de MinIO
def delete_old_files_from_minio():
    # Obtener la conexión de MinIO desde Airflow
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)

    # Crear el cliente de MinIO/S3 con las credenciales y configuración necesarias
    s3_client = get_minio_client()


    bucket_name = 'tmp'
    expiration_time = datetime.utcnow() - timedelta(hours=24)

    # Listar todos los objetos en el bucket
    objects = s3_client.list_objects_v2(Bucket=bucket_name)
    
    if 'Contents' in objects:
        for obj in objects['Contents']:
            # Convertir el tiempo de la última modificación al tiempo UTC
            last_modified = obj['LastModified'].replace(tzinfo=None)
            # Eliminar el archivo si es más antiguo que 24 horas
            if last_modified < expiration_time:
                print(f"Eliminando {obj['Key']}...")
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                print(f"{obj['Key']} eliminado correctamente.")
    else:
        print("No se encontraron objetos en el bucket.")

default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 29),  # Fecha específica en la que quieres que se ejecute
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'function_minio_delete_old_temp_files',
    default_args=default_args,
    description='DAG para eliminar archivos antiguos en MinIO',
    schedule_interval=None,  # No se ejecuta periódicamente, solo una vez
    catchup=False,  # No se ejecuta automáticamente para fechas pasadas
)

delete_old_files_task = PythonOperator(
    task_id='delete_old_files',
    python_callable=delete_old_files_from_minio,
    dag=dag,
)

delete_old_files_task