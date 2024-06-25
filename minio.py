from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os

# Función para procesar la imagen
def process_image():
    # Aquí es donde deberías tener la lógica para recibir la imagen
    image_path = 'https://www.carype.com/frontend/img/upload/dummy-playmobil-dummy.jpg'
    return image_path

# Función para subir la imagen a MinIO
def upload_to_minio(image_path):
    hook = S3Hook(aws_conn_id='aws_default')
    hook.load_file(
        filename=image_path,
        key='your-image-key.jpg',
        bucket_name='bucket-test',
        replace=True,
        encrypt=False,
        endpoint_url='https://storage-minio.default.svc.cluster.local:9000'  # Añade la URL de MinIO aquí
    )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'save_image_to_minio',
    default_args=default_args,
    description='A simple DAG to save an image to MinIO',
    schedule_interval='@once',
    catchup=False,
) as dag:

    # Tarea para procesar la imagen
    process_image_task = PythonOperator(
        task_id='process_image',
        python_callable=process_image
    )

    # Tarea para subir la imagen a MinIO
    upload_to_minio_task = PythonOperator(
        task_id='upload_to_minio',
        python_callable=lambda **kwargs: upload_to_minio(kwargs['ti'].xcom_pull(task_ids='process_image'))
    )

    process_image_task >> upload_to_minio_task
