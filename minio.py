from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

# Función para procesar la imagen
def process_image():
    image_url = 'https://www.carype.com/frontend/img/upload/dummy-playmobil-dummy.jpg'
    local_path = '/tmp/dummy-playmobil-dummy.jpg'

    # Descargar la imagen
    response = requests.get(image_url)
    if response.status_code == 200:
        with open(local_path, 'wb') as f:
            f.write(response.content)
    else:
        raise Exception(f"Failed to download image. Status code: {response.status_code}")

    return local_path

# Función para subir la imagen a MinIO
def upload_to_minio(image_path):
    hook = S3Hook(aws_conn_id='minio_conn')
    hook.load_file(
        filename=image_path,
        key='your-image-key.jpg',
        bucket_name='bucket-test',
        replace=True,
        encrypt=False
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




