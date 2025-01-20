import os
import json
import tempfile
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
import boto3
from botocore.client import Config
from moviepy.editor import VideoFileClip
import base64
import json
import os
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from flask import Config
import requests
import logging
import io  # Para manejar el archivo XML en memoria
from pyproj import Proj, transform, CRS
import re
from airflow.hooks.base import BaseHook
import boto3
from PIL import Image
import os
import os
import base64
import tempfile
import uuid
import json
import boto3
from botocore.config import Config
from airflow.hooks.base_hook import BaseHook
from PIL import Image
import base64
import io
import logging
import requests
from airflow.hooks.base import BaseHook
def scan_minio_for_videos(**kwargs):

    """ Escanea un bucket de MinIO para detectar nuevos videos."""  
    
    # Conexión a MinIO
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

    # Escaneo del bucket
    bucket_name = 'temp'  # Cambia según tu bucket
    paginator = s3_client.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucket_name)

    # Filtrar videos
    new_videos = []
    for page in result:
        for content in page.get('Contents', []):
            if content['Key'].endswith('.mp4'):
                new_videos.append(content['Key'])

    print(f"Nuevos videos detectados: {new_videos}")
    kwargs['task_instance'].xcom_push(key='new_videos', value=new_videos)

def process_and_generate_thumbnail(**kwargs):
    """
    Descarga videos, genera miniaturas y las sube a MinIO.
    """
    # Obtener videos detectados
    videos = kwargs['task_instance'].xcom_pull(key='new_videos', default=[])
    if not videos:
        print("No hay nuevos videos para procesar.")
        return

    # Conexión a MinIO
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

    bucket_name = 'temp'  # Cambia según tu bucket

    for video_key in videos:
        print(f"Procesando video: {video_key}")

        # Preparar rutas temporales
        temp_dir = tempfile.mkdtemp()
        video_path = os.path.join(temp_dir, "video.mp4")
        thumbnail_path = os.path.join(temp_dir, "thumbs.jpg")
        thumbnail_key = os.path.join(os.path.dirname(video_key), "thumbs.jpg")

        try:
            # Descargar el video desde MinIO
            print(f"Descargando video desde MinIO: {video_key}...")
            s3_client.download_file(bucket_name, video_key, video_path)
            print(f"Video descargado correctamente: {video_path}")

            # Generar el thumbnail
            print(f"Generando miniatura para {video_path}...")
            with VideoFileClip(video_path) as video:
                video.save_frame(thumbnail_path, t=10)  # Captura en el segundo 10
            print(f"Miniatura generada: {thumbnail_path}")

            # Subir el thumbnail a MinIO
            print(f"Subiendo miniatura a MinIO: {thumbnail_key}...")
            s3_client.upload_file(thumbnail_path, bucket_name, thumbnail_key)
            print(f"Miniatura subida correctamente: {thumbnail_key}")

        except Exception as e:
            print(f"Error procesando {video_key}: {e}")
        finally:
            # Limpieza de archivos temporales
            print("Limpiando archivos temporales...")
            if os.path.exists(video_path):
                os.remove(video_path)
            if os.path.exists(thumbnail_path):
                os.remove(thumbnail_path)
            os.rmdir(temp_dir)

# Configuración del DAG
default_args = {
    'owner': 'thumbnail_generator',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'scan_minio_and_generate_thumbnails',
    default_args=default_args,
    description='Escanea MinIO para videos y genera miniaturas',
    schedule_interval='*/1 * * * *',
    catchup=False,
)


scan_minio_task = PythonOperator(
    task_id='scan_minio',
    python_callable=scan_minio_for_videos,
    provide_context=True,
    dag=dag,
)

process_videos_task = PythonOperator(
    task_id='process_videos',
    python_callable=process_and_generate_thumbnail,
    provide_context=True,
    dag=dag,
)

scan_minio_task >> process_videos_task
