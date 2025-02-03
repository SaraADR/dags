import os
import json
import tempfile
import uuid
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
import boto3
from botocore.client import Config
from moviepy import VideoFileClip, ImageClip

# ----------- FUNCIONES AUXILIARES -----------

def load_processed_files_from_minio(s3_client, bucket_name, key):
    """Carga la lista de archivos procesados desde MinIO."""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except s3_client.exceptions.NoSuchKey:
        return []
    except Exception as e:
        print(f"[ERROR] No se pudo cargar {key} desde MinIO: {e}")
        return []

def save_processed_files_to_minio(s3_client, bucket_name, key, processed_files):
    """Guarda la lista de archivos procesados en MinIO."""
    try:
        json_data = json.dumps(processed_files, indent=4)
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json_data)
    except Exception as e:
        print(f"[ERROR] No se pudo guardar {key} en MinIO: {e}")

# ----------- ESCANEO DE MINIO -----------

def scan_minio_for_files(**kwargs):
    """Escanea MinIO en busca de nuevos videos e imágenes."""
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
    
    processed_videos = load_processed_files_from_minio(s3_client, bucket_name, 'processed_videos.json')
    processed_images = load_processed_files_from_minio(s3_client, bucket_name, 'processed_images.json')

    processed_video_keys = [entry['key'] for entry in processed_videos]
    processed_image_keys = [entry['key'] for entry in processed_images]

    paginator = s3_client.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucket_name)

    new_videos, new_images = [], []
    for page in result:
        for content in page.get('Contents', []):
            file_key = content['Key']
            if file_key.endswith('.mp4') and file_key not in processed_video_keys:
                new_videos.append(file_key)
            elif file_key.endswith(('.png', '.jpg', '.jpeg', '.tiff')) and file_key not in processed_image_keys:
                new_images.append(file_key)

    print(f"[INFO] Nuevos videos detectados: {new_videos}")
    print(f"[INFO] Nuevas imágenes detectadas: {new_images}")

    kwargs['task_instance'].xcom_push(key='new_videos', value=new_videos)
    kwargs['task_instance'].xcom_push(key='new_images', value=new_images)

# ----------- PROCESAMIENTO DE VIDEOS CON REINTENTO -----------

def process_and_generate_video_thumbnails(**kwargs):
    """Procesa videos detectados, genera miniaturas y las sube a MinIO."""
    videos = kwargs['task_instance'].xcom_pull(key='new_videos', default=[])
    if not videos:
        print("[INFO] No hay nuevos videos para procesar.")
        return

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
    processed_file_key = 'processed_videos.json'
    failed_file_key = 'failed_videos.json'
    
    processed_videos = load_processed_files_from_minio(s3_client, bucket_name, processed_file_key)
    failed_videos = load_processed_files_from_minio(s3_client, bucket_name, failed_file_key)

    for video_key in videos:
        print(f"[INFO] Procesando video: {video_key}")

        temp_dir = tempfile.mkdtemp()
        video_path = os.path.join(temp_dir, "video.mp4")
        thumbnail_path = os.path.join(temp_dir, "thumb.jpg")
        thumbnail_key = os.path.join("thumbs", os.path.basename(video_key).replace('.mp4', '_thumb.jpg'))

        max_retries = 3
        for attempt in range(max_retries):
            try:
                s3_client.download_file(bucket_name, video_key, video_path)
                with VideoFileClip(video_path) as video:
                    video.save_frame(thumbnail_path, t=10)

                s3_client.upload_file(thumbnail_path, bucket_name, thumbnail_key)
                processed_videos.append({"key": video_key, "uuid": str(uuid.uuid4())})
                break  

            except Exception as e:
                print(f"[WARNING] Fallo al procesar {video_key} (Intento {attempt + 1}/{max_retries}): {e}")
                time.sleep(5)
        else:
            print(f"[ERROR] No se pudo procesar {video_key}. Guardando en 'failed_videos.json'")
            failed_videos.append(video_key)

        os.remove(video_path) if os.path.exists(video_path) else None
        os.remove(thumbnail_path) if os.path.exists(thumbnail_path) else None
        os.rmdir(temp_dir)

    save_processed_files_to_minio(s3_client, bucket_name, processed_file_key, processed_videos)
    save_processed_files_to_minio(s3_client, bucket_name, failed_file_key, failed_videos)

# ----------- PROCESAMIENTO DE IMÁGENES CON REINTENTO -----------

def process_and_generate_image_thumbnails(**kwargs):
    """Procesa imágenes detectadas, genera miniaturas y las sube a MinIO."""
    images = kwargs['task_instance'].xcom_pull(key='new_images', default=[])
    if not images:
        print("[INFO] No hay nuevas imágenes para procesar.")
        return

    # Aplicar la misma lógica de reintentos y almacenamiento de fallos que en videos

# ----------- CONFIGURACIÓN DEL DAG -----------

default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'scan_minio_and_generate_thumbnails',
    default_args=default_args,
    description='Escanea MinIO y genera miniaturas con reintentos',
    schedule_interval='*/3 * * * *',
    catchup=False,
    max_active_runs=1,  
    concurrency=3  
)

scan_task = PythonOperator(
    task_id='scan_minio',
    python_callable=scan_minio_for_files,
    provide_context=True,
    dag=dag,
)

process_videos_task = PythonOperator(
    task_id='process_videos',
    python_callable=process_and_generate_video_thumbnails,
    provide_context=True,
    dag=dag,
)

process_images_task = PythonOperator(
    task_id='process_images',
    python_callable=process_and_generate_image_thumbnails,
    provide_context=True,
    dag=dag,
)

scan_task >> [process_videos_task, process_images_task]
