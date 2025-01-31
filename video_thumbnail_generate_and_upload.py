import os
import json
import tempfile
import uuid
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
        print(f"Error al cargar archivos procesados: {e}")
        return []

def save_processed_files_to_minio(s3_client, bucket_name, key, processed_files):
    """Guarda la lista de archivos procesados en MinIO."""
    try:
        json_data = json.dumps(processed_files, indent=4)
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json_data)
    except Exception as e:
        print(f"Error al guardar archivos procesados: {e}")

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

    bucket_name = 'temp'
    
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

    print(f"Nuevos videos detectados: {new_videos}")
    print(f"Nuevas imágenes detectadas: {new_images}")

    kwargs['task_instance'].xcom_push(key='new_videos', value=new_videos)
    kwargs['task_instance'].xcom_push(key='new_images', value=new_images)

# ----------- PROCESAMIENTO DE VIDEOS -----------

def process_and_generate_video_thumbnails(**kwargs):
    """Procesa videos detectados, genera miniaturas y las sube a MinIO."""
    videos = kwargs['task_instance'].xcom_pull(key='new_videos', default=[])
    if not videos:
        print("No hay nuevos videos para procesar.")
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

    bucket_name = 'temp'
    processed_file_key = 'processed_videos.json'
    processed_videos = load_processed_files_from_minio(s3_client, bucket_name, processed_file_key)

    for video_key in videos:
        print(f"Procesando video: {video_key}")

        temp_dir = tempfile.mkdtemp()
        video_path = os.path.join(temp_dir, "video.mp4")
        thumbnail_path = os.path.join(temp_dir, "thumbs.jpg")
        thumbnail_key = os.path.join("temp/thumbs", os.path.basename(video_key).replace('.mp4', 'thumb.jpg'))

        try:
            s3_client.download_file(bucket_name, video_key, video_path)
            with VideoFileClip(video_path) as video:
                video.save_frame(thumbnail_path, t=10)

            s3_client.upload_file(thumbnail_path, bucket_name, thumbnail_key)
            processed_videos.append({"key": video_key, "uuid": str(uuid.uuid4())})

        except Exception as e:
            print(f"Error procesando {video_key}: {e}")
        finally:
            os.remove(video_path) if os.path.exists(video_path) else None
            os.remove(thumbnail_path) if os.path.exists(thumbnail_path) else None
            os.rmdir(temp_dir)

    save_processed_files_to_minio(s3_client, bucket_name, processed_file_key, processed_videos)

# ----------- PROCESAMIENTO DE IMÁGENES -----------

def process_and_generate_image_thumbnails(**kwargs):
    """Procesa imágenes detectadas, genera miniaturas y las sube a MinIO."""
    images = kwargs['task_instance'].xcom_pull(key='new_images', default=[])
    if not images:
        print("No hay nuevas imágenes para procesar.")
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

    bucket_name = 'temp'
    processed_file_key = 'processed_images.json'
    processed_images = load_processed_files_from_minio(s3_client, bucket_name, processed_file_key)

    for image_key in images:
        print(f"Procesando imagen: {image_key}")

        temp_dir = tempfile.mkdtemp()
        image_path = os.path.join(temp_dir, "image")
        thumbnail_path = os.path.join(temp_dir, "thumbs.jpg")
        thumbnail_key = os.path.join("temp/thumbs", os.path.basename(image_key).replace('.jpg', 'thumb.jpg'))

        try:
            s3_client.download_file(bucket_name, image_key, image_path)
            clip = ImageClip(image_path)
            clip.save_frame(thumbnail_path)

            s3_client.upload_file(thumbnail_path, bucket_name, thumbnail_key)
            processed_images.append({"key": image_key, "uuid": str(uuid.uuid4())})

        except Exception as e:
            print(f"Error procesando {image_key}: {e}")
        finally:
            os.remove(image_path) if os.path.exists(image_path) else None
            os.remove(thumbnail_path) if os.path.exists(thumbnail_path) else None
            os.rmdir(temp_dir)

    save_processed_files_to_minio(s3_client, bucket_name, processed_file_key, processed_images)

# ----------- CONFIGURACIÓN DEL DAG -----------

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
    description='Escanea MinIO y genera miniaturas',
    schedule_interval='*/1 * * * *',
    catchup=False,
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
