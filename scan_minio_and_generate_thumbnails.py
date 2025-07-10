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
from moviepy import VideoFileClip
from dag_utils import get_minio_client
from PIL import Image, ImageSequence  # Para procesar imágenes con Pillow


# ----------- FUNCIONES AUXILIARES -----------

def load_processed_files_from_minio(s3_client, bucket_name, key):
    """
    Carga la lista de archivos procesados desde MinIO.
    Si no existe el archivo, retorna lista vacía.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except s3_client.exceptions.NoSuchKey:
        return []
    except Exception as e:
        print(f"Error al cargar archivos procesados: {e}")
        return []

def save_processed_files_to_minio(s3_client, bucket_name, key, processed_files):
    """
    Guarda la lista actualizada de archivos procesados en MinIO.
    """
    try:
        json_data = json.dumps(processed_files, indent=4)
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json_data)
    except Exception as e:
        print(f"Error al guardar archivos procesados: {e}")

# ----------- ESCANEO DE MINIO -----------

def scan_minio_for_files(**kwargs):
    """
    Escanea el bucket de MinIO para detectar nuevos videos e imágenes que no hayan sido procesados.
    Guarda las listas nuevas en XCom para tareas siguientes.
    """
    s3_client = get_minio_client()
    bucket_name = 'tmp'
    
    processed_videos = load_processed_files_from_minio(s3_client, bucket_name, 'processed_videos.json')
    processed_images = load_processed_files_from_minio(s3_client, bucket_name, 'processed_images.json')

    # Extraer keys ya procesados para evitar reprocesamiento
    processed_video_keys = [entry['key'] for entry in processed_videos]
    processed_image_keys = [entry['key'] for entry in processed_images]

    paginator = s3_client.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucket_name)

    new_videos, new_images = [], []
    for page in result:
        for content in page.get('Contents', []):
            file_key = content['Key']
            # Filtrar nuevos archivos por extensión y que no estén ya procesados
            if file_key.endswith('.mp4') and file_key not in processed_video_keys:
                new_videos.append(file_key)
            elif file_key.endswith(('.png', '.jpg', '.jpeg', '.tiff')) and file_key not in processed_image_keys:
                new_images.append(file_key)

    print(f"Nuevos videos detectados: {new_videos}")
    print(f"Nuevas imágenes detectadas: {new_images}")

    # Guardar resultados en XCom para tareas dependientes
    kwargs['task_instance'].xcom_push(key='new_videos', value=new_videos)
    kwargs['task_instance'].xcom_push(key='new_images', value=new_images)

# ----------- PROCESAMIENTO DE VIDEOS -----------

def process_and_generate_video_thumbnails(**kwargs):
    """
    Para cada video nuevo detectado:
    - Descarga el video.
    - Extrae un frame a los 10 segundos para crear la miniatura.
    - Sube la miniatura a MinIO en carpeta /thumbs.
    - Actualiza la lista de videos procesados.
    """
    videos = kwargs['task_instance'].xcom_pull(key='new_videos', default=[])
    if not videos:
        print("No hay nuevos videos para procesar.")
        return

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
            # Descargar video
            s3_client.download_file(bucket_name, video_key, video_path)
            # Abrir video y guardar frame en el tiempo t=10s
            with VideoFileClip(video_path) as video:
                video.save_frame(thumbnail_path, t=10)

            # Subir miniatura
            s3_client.upload_file(thumbnail_path, bucket_name, thumbnail_key)
            # Registrar video procesado
            processed_videos.append({"key": video_key, "uuid": str(uuid.uuid4())})

        except Exception as e:
            print(f"Error procesando {video_key}: {e}")
        finally:
            # Limpiar archivos temporales
            if os.path.exists(video_path):
                os.remove(video_path)
            if os.path.exists(thumbnail_path):
                os.remove(thumbnail_path)
            os.rmdir(temp_dir)

    # Guardar lista actualizada de videos procesados
    save_processed_files_to_minio(s3_client, bucket_name, processed_file_key, processed_videos)

# ----------- PROCESAMIENTO DE IMÁGENES -----------

def process_and_generate_image_thumbnails(**kwargs):
    """
    Para cada imagen nueva detectada:
    - Descarga la imagen.
    - Si es TIFF en modo I;16, convierte a 8 bits escala de grises.
    - Convierte otras imágenes a RGB si es necesario.
    - Guarda miniatura JPG redimensionada (128x128).
    - Sube la miniatura a /thumbs.
    - Actualiza la lista de imágenes procesadas.
    """
    images = kwargs['task_instance'].xcom_pull(key='new_images', default=[])
    if not images:
        print("No hay nuevas imágenes para procesar.")
        return

    s3_client = get_minio_client()
    bucket_name = 'tmp'
    processed_file_key = 'processed_images.json'
    processed_images = load_processed_files_from_minio(s3_client, bucket_name, processed_file_key)

    for image_key in images:
        # Evitar reprocesar miniaturas ya en /thumbs o con sufijo _thumb
        if "/thumbs/" in image_key or "_thumb" in image_key:
            continue

        if image_key in [img['key'] for img in processed_images]:
            continue

        print(f"Procesando imagen: {image_key}")

        temp_dir = tempfile.mkdtemp()
        original_extension = os.path.splitext(image_key)[-1].lower()
        image_path = os.path.join(temp_dir, f"image{original_extension}")
        thumbnail_path = os.path.join(temp_dir, "thumb.jpg")

        base_name = os.path.splitext(os.path.basename(image_key))[0]
        thumbnail_key = os.path.join("thumbs", f"{base_name}_thumb.jpg")

        try:
            s3_client.download_file(bucket_name, image_key, image_path)

            with Image.open(image_path) as img:
                # Procesar TIFF con modo I;16
                if original_extension in ['.tiff', '.tif'] and img.mode == 'I;16':
                    # Escalar de 16 bits a 8 bits y convertir a escala de grises
                    img = img.point(lambda i: i * (1./256)).convert('L')
                else:
                    # Convertir a RGB si no está en ese modo para JPEG
                    if img.mode != 'RGB':
                        img = img.convert('RGB')

                # Redimensionar manteniendo proporción y máximo 128x128
                img.thumbnail((512, 512))

                # Guardar como JPEG calidad buena
                img.save(thumbnail_path, format='JPEG', quality=90, optimize=True)

            s3_client.upload_file(thumbnail_path, bucket_name, thumbnail_key)
            print(f"Miniatura JPG subida a /thumbs: {thumbnail_key}")

            processed_images.append({"key": image_key, "uuid": str(uuid.uuid4())})

        except Exception as e:
            print(f"Error procesando {image_key}: {e}")
        finally:
            if os.path.exists(image_path):
                os.remove(image_path)
            if os.path.exists(thumbnail_path):
                os.remove(thumbnail_path)
            os.rmdir(temp_dir)

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
    schedule_interval='*/1 * * * *',  # Cada minuto
    catchup=False,
    max_active_runs=1,
)
# Definición de tareas

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

# Definir el flujo: primero escaneo, luego procesamiento paralelo videos e imágenes
scan_task >> [process_videos_task, process_images_task]