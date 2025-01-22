import os
import json
import tempfile
import uuid  # Importar módulo UUID
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
import boto3
from botocore.client import Config
from moviepy import VideoFileClip

def load_processed_videos_from_minio(s3_client, bucket_name, key):
    """
    Carga la lista de videos procesados desde MinIO.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        processed_videos = json.loads(response['Body'].read().decode('utf-8'))
        return processed_videos
    except s3_client.exceptions.NoSuchKey:
        print(f"No se encontró el archivo {key} en el bucket {bucket_name}. Creando lista vacía.")
        return []
    except Exception as e:
        print(f"Error al cargar videos procesados: {e}")
        return []

def save_processed_videos_to_minio(s3_client, bucket_name, key, processed_videos):
    """
    Guarda la lista de videos procesados en MinIO.
    """
    try:
        json_data = json.dumps(processed_videos, indent=4)  # Mejorar legibilidad
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json_data)
        print(f"Lista de videos procesados guardada en {key}.")
    except Exception as e:
        print(f"Error al guardar videos procesados: {e}")

def scan_minio_for_videos(**kwargs):
    """
    Escanea un bucket de MinIO para detectar nuevos videos.
    """
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

    # Parámetros del bucket y archivo de procesados
    bucket_name = 'temp'  # Cambiar según tu configuración
    processed_file_key = 'processed_videos.json'

    # Cargar videos procesados desde MinIO
    processed_videos = load_processed_videos_from_minio(s3_client, bucket_name, processed_file_key)
    processed_keys = [entry['key'] for entry in processed_videos]  # Extraer solo claves de los procesados
    print(f"Videos procesados previamente: {processed_keys}")

    # Escaneo del bucket
    paginator = s3_client.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucket_name)

    # Filtrar videos no procesados
    new_videos = []
    for page in result:
        for content in page.get('Contents', []):
            video_key = content['Key']
            if video_key.endswith('.mp4') and video_key not in processed_keys:
                new_videos.append(video_key)

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

    bucket_name = 'temp'  # Cambiar según tu configuración
    processed_file_key = 'processed_videos.json'

    # Cargar lista de videos procesados desde MinIO
    processed_videos = load_processed_videos_from_minio(s3_client, bucket_name, processed_file_key)

    for video_key in videos:
        print(f"Procesando video: {video_key}")

        # Preparar rutas temporales
        temp_dir = tempfile.mkdtemp()
        video_path = os.path.join(temp_dir, "video.mp4")
        thumbnail_path = os.path.join(temp_dir, "thumbs.jpg")
        thumbnail_key = os.path.join(os.path.dirname(video_key), "thumbs.jpg")

        try:
            # Descargar el video desde MinIO
            s3_client.download_file(bucket_name, video_key, video_path)

            # Generar el thumbnail
            with VideoFileClip(video_path) as video:
                video.save_frame(thumbnail_path, t=10)  # Captura en el segundo 10

            # Subir el thumbnail a MinIO
            s3_client.upload_file(thumbnail_path, bucket_name, thumbnail_key)

            # Registrar el video como procesado con UUID
            processed_videos.append({
                "key": video_key,
                "uuid": str(uuid.uuid4())  # Generar UUID único
            })

        except Exception as e:
            print(f"Error procesando {video_key}: {e}")
        finally:
            # Limpieza de archivos temporales
            if os.path.exists(video_path):
                os.remove(video_path)
            if os.path.exists(thumbnail_path):
                os.remove(thumbnail_path)
            os.rmdir(temp_dir)

    # Guardar la lista actualizada en MinIO
    save_processed_videos_to_minio(s3_client, bucket_name, processed_file_key, processed_videos)

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
