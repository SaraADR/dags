import json
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
from botocore.client import Config, ClientError
from airflow.hooks.base_hook import BaseHook

def create_bucket_in_minio(bucket_name):
    # Obtener la conexión de MinIO desde Airflow
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)

    logging.info("Conexión a MinIO obtenida.")

    # Crear el cliente de MinIO/S3 con las credenciales y configuración necesarias
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

    logging.info(f"Intentando crear el bucket '{bucket_name}' en MinIO.")

    # Crear el bucket si no existe
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logging.info(f"El bucket '{bucket_name}' ya existe.")
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            # El bucket no existe, así que procedemos a crearlo
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                logging.info(f"Bucket '{bucket_name}' creado exitosamente en MinIO.")
            except Exception as create_err:
                logging.error(f"Error al crear el bucket: {str(create_err)}")
                raise
        else:
            logging.error(f"Error al verificar el bucket: {str(e)}")
            raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'create_bucket_in_minio',
    default_args=default_args,
    description='Un DAG para crear un bucket en Minio',
    schedule_interval=None,  # No se repite automáticamente
    catchup=False,  # Evita la ejecución retroactiva de períodos anteriores
)

create_bucket_task = PythonOperator(
    task_id='create_minio_bucket',
    python_callable=create_bucket_in_minio,
    op_args=['your_bucket_name'],  # Reemplaza con el nombre del bucket que quieras crear
    dag=dag,
)

create_bucket_task
