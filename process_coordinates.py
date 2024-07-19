import io
import tempfile
import uuid
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import boto3
from botocore.client import Config
from airflow.hooks.base_hook import BaseHook

def process_kafka_message(**context):
    # Extraer el mensaje del contexto de Airflow
    message = context['dag_run'].conf

    if not message:
        raise KeyError("No message found in the DAG run configuration.")
    
    # Si el mensaje no es un JSON, se toma como string directamente
    message_content = message.get('message', None)
    
    if not message_content:
        raise ValueError("No valid 'message' key found in the DAG run configuration.")

    unique_id = str(uuid.uuid4())

    if message_content:
        # Suponemos que el contenido del archivo viene como una cadena codificada en latin1
        file_bytes = message_content.encode('latin1')

        # Mostrar los primeros 40 bytes del contenido del archivo para verificación
        print(f"Received file content (first 40 bytes): {file_bytes[:40]}")

        # Crear un directorio temporal utilizando el módulo tempfile
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_pdf_path = os.path.join(temp_dir, 'file.pdf')

            # Guardar el contenido en un archivo PDF temporal
            with open(temp_pdf_path, 'wb') as temp_pdf_file:
                temp_pdf_file.write(file_bytes)

            print(f"PDF temporal creado en {temp_pdf_path}")

            # Subir el PDF a MinIO
            save_to_minio(temp_pdf_path, unique_id)

            print(f"PDF subido a MinIO con ID único {unique_id}")

def save_to_minio(file_path, unique_id):
    # Obtener la conexión de MinIO desde Airflow
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)

    # Crear el cliente de MinIO/S3 con las credenciales y configuración necesarias
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

    bucket_name = 'locationtest'
    file_name = os.path.basename(file_path)

    # Crear el bucket si no existe
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except s3_client.exceptions.NoSuchBucket:
        s3_client.create_bucket(Bucket=bucket_name)

    # Leer el contenido del archivo desde el sistema de archivos
    with open(file_path, 'rb') as file:
        file_content = file.read()

    # Subir el archivo a MinIO
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=file_content,
        Tagging=f"unique_id={unique_id}"
    )
    print(f'{file_name} subido correctamente a MinIO.')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'save_coordinates_to_minio',
    default_args=default_args,
    description='Un DAG para almacenar coordenadas en Minio',
    schedule_interval=timedelta(days=1),
)

save_task = PythonOperator(
    task_id='process_coordinates',
    provide_context=True,
    python_callable=process_kafka_message,
    dag=dag,
)

save_task
