import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from minio import Minio
from minio.error import S3Error

# Función para crear un bucket en MinIO
def create_bucket_in_minio(**kwargs):
    bucket_name = kwargs['bucket_name']
    minio_client = Minio(
        endpoint=os.environ['MINIO_ENDPOINT'],
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY'],
        secure=False
    )

    # Crear el bucket si no existe
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        print(f"Bucket {bucket_name} created successfully.")
    else:
        print(f"Bucket {bucket_name} already exists.")

# Definir el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'create_bucket_in_minio',
    default_args=default_args,
    description='A simple DAG to create a bucket in MinIO',
    schedule_interval=timedelta(days=1),
)

# Definir el nombre del bucket
bucket_name = 'new_bucket'

# Crear la tarea para crear el bucket
create_bucket_task = PythonOperator(
    task_id='create_bucket_in_minio_task',
    python_callable=create_bucket_in_minio,
    op_kwargs={
        'bucket_name': bucket_name,
    },
    dag=dag,
)
