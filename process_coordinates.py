import io
import json
import tempfile
import uuid
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import os
import boto3
from botocore.client import Config
from airflow.hooks.base_hook import BaseHook

def process_kafka_message(**context):
    try:
        message = context['dag_run'].conf
        if not message:
            raise KeyError("No message found in the DAG run configuration.")
    except KeyError:
        raise KeyError("No configuration found in the DAG run configuration.")
    
    try:
        message_content = message.get('message', None)
        if not message_content:
            raise ValueError("No valid 'message' key found in the DAG run configuration.")
    except KeyError:
        raise ValueError("No valid 'message' key found in the DAG run configuration.")

    unique_id = str(uuid.uuid4())

    # Verificar si el contenido del mensaje es un diccionario y convertir a string si es necesario
    if isinstance(message_content, dict):
        # Aquí podrías transformar el diccionario a JSON o procesarlo de otra manera si es necesario
        file_bytes = json.dumps(message_content).encode('utf-8')
    elif isinstance(message_content, str):
        file_bytes = message_content.encode('utf-8')
    else:
        raise ValueError("The message content must be a string or a dictionary.")

    temp_data_path = os.path.join(tempfile.gettempdir(), f"{unique_id}_data.json")

    with open(temp_data_path, 'wb') as temp_data_file:
        temp_data_file.write(file_bytes)

    return temp_data_path, unique_id

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

process_message_task = PythonOperator(
    task_id='process_kafka_message',
    provide_context=True,
    python_callable=process_kafka_message,
    dag=dag,
)

docker_task = DockerOperator(
    task_id='generate_pdf',
    image='pdf-generator',  # Reemplaza con el nombre de tu imagen Docker
    api_version='auto',
    auto_remove=True,
    command='python generate_pdf.py /data/{{ ti.xcom_pull(task_ids="process_kafka_message")[0] }} /data/output.pdf',
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    volumes=['/tmp:/data'],
    dag=dag,
)

save_to_minio_task = PythonOperator(
    task_id='save_to_minio',
    provide_context=True,
    python_callable=save_to_minio,
    op_args=['/tmp/output.pdf', '{{ ti.xcom_pull(task_ids="process_kafka_message")[1] }}'],
    dag=dag,
)

process_message_task >> docker_task >> save_to_minio_task
