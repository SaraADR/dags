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
from dag_utils import get_minio_client

# ----------- FUNCIONES AUXILIARES -----------

def load_processed_files_from_minio(s3_client, bucket_name, key):
    """Carga la lista de archivos procesados desde MinIO."""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except s3_client.exceptions.NoSuchKey:
        return []
    except Exception as e:
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

    # Import de la función get_s3_client
    s3_client = get_minio_client()

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


    kwargs['task_instance'].xcom_push(key='new_videos', value=new_videos)
    kwargs['task_instance'].xcom_push(key='new_images', value=new_images)

# ----------- PROCESAMIENTO DE VIDEOS -----------

def process_and_generate_video_thumbnails(**kwargs):
    """Procesa videos detectados, genera miniaturas y las sube a MinIO."""
    videos = kwargs['task_instance'].xcom_pull(key='new_videos', default=[])
    if not videos:
        print("No hay nuevos videos para procesar.")
        return

    # Import de la función get_s3_client
    s3_client = get_minio_client()

    bucket_name = 'tmp'
    processed_file_key = 'processed_videos.json'
    processed_videos = load_processed_files_from_minio(s3_client, bucket_name, processed_file_key)

    for video_key in videos:
        print(f"Procesando video: {video_key}")

        temp_dir = tempfile.mkdtemp()
        video_path = os.path.join(temp_dir, "video.mp4")
        base_name = os.path.splitext(os.path.basename(video_key))[0]
        thumbnail_path = os.path.join(temp_dir, f"{base_name}_thumb.jpg")
        thumbnail_key = os.path.join("/thumbs", f"{base_name}_thumb.jpg")

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
    """Procesa imágenes detectadas, genera miniaturas y mueve ambas a la carpeta /thumbs en MinIO."""
    images = kwargs['task_instance'].xcom_pull(key='new_images', default=[])
    if not images:
        return

    s3_client = get_minio_client()
    bucket_name = 'tmp'
    processed_file_key = 'processed_images.json'
    processed_images = load_processed_files_from_minio(s3_client, bucket_name, processed_file_key)

    for image_key in images:
        # Evitar procesar miniaturas dentro de /thumbs
        if "/thumbs/" in image_key or "_thumb" in image_key:
            continue

        # Verificar si la imagen ya ha sido procesada
        if image_key in [img['key'] for img in processed_images]:
            continue


        temp_dir = tempfile.mkdtemp()
        original_extension = os.path.splitext(image_key)[-1].lower()
        image_path = os.path.join(temp_dir, f"image{original_extension}")
        thumbnail_path = os.path.join(temp_dir, "thumb.jpg")

        # Generar rutas para la carpeta /thumbs
        base_name = os.path.splitext(os.path.basename(image_key))[0]
        original_in_thumbs_key = os.path.join("thumbs", os.path.basename(image_key))  # Original en /thumbs
        thumbnail_key = os.path.join("thumbs", f"{base_name}_thumb.jpg")  # Miniatura en /thumbs

        try:
            # Descargar archivo original desde MinIO
            s3_client.download_file(bucket_name, image_key, image_path)

            # Subir el archivo original a la carpeta /thumbs (si aún no existe)
            if not any(img['key'] == original_in_thumbs_key for img in processed_images):
                s3_client.upload_file(image_path, bucket_name, original_in_thumbs_key)

            # Procesar la imagen para generar una miniatura
            clip = ImageClip(image_path)
            clip.save_frame(thumbnail_path)  # Generar miniatura

            # Subir la miniatura a la carpeta /thumbs
            s3_client.upload_file(thumbnail_path, bucket_name, thumbnail_key)

            # Registrar el archivo procesado
            processed_images.append({"key": image_key, "uuid": str(uuid.uuid4())})

        except Exception as e:
            print(f"Error procesando {image_key}: {e}")
        finally:
            # Limpieza de archivos temporales
            os.remove(image_path) if os.path.exists(image_path) else None
            os.remove(thumbnail_path) if os.path.exists(thumbnail_path) else None
            os.rmdir(temp_dir)

    # Guardar el registro actualizado de archivos procesados en MinIO
    save_processed_files_to_minio(s3_client, bucket_name, processed_file_key, processed_images)



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
    description='Escanea MinIO y genera miniaturas',
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1,    
    
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
