from datetime import datetime, timedelta
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.hooks.base import BaseHook
import boto3
from botocore.client import Config
import base64
import json
import os

# Función para manejar la conexión a MinIO y subir archivos
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


def print_message(**context):
    message = context['dag_run'].conf
    if message is not None:
        print(f"El mensaje se ha recibido correctamente")

# Define the actions based on the last digit of Gimbal Tilt
def handle_gimbal_tilt(gimbal_tilt):
    last_digit = gimbal_tilt[-1]  # Get the last character
    if last_digit == '1':
        print("Gimbal Tilt ends with 1: Creating resource.")
    elif last_digit == '5':
        print("Gimbal Tilt ends with 5: Waiting...")
    elif last_digit == '9':
        print("Gimbal Tilt ends with 9: Closing operation.")
    else:
        print(f"Gimbal Tilt ends with {last_digit}: No specific action defined.")

# Define a function to parse metadata
def parse_metadata(metadata):
    data = {}
    # Each line represents a key-value pair in the format "Key : Value"
    for line in metadata.splitlines():
        if ' : ' in line:
            key, value = line.split(' : ', 1)
            data[key.strip()] = value.strip()
    return data



# Function to process metadata
def process_metadata(**kwargs):
    ti = kwargs['ti']
    metadata = ti.xcom_pull(task_ids='run_docker')
    print(f"Metadata received: {metadata}")

    decoded_bytes = base64.b64decode(metadata)
    decoded_str = decoded_bytes.decode('utf-8')
    # Apply the parse function
    metadata_dict = parse_metadata(decoded_str)

    # Print the metadata in JSON format
    print(f"Metadata received:\n{json.dumps(metadata_dict, indent=4)}")

    # Specifically print the "Gimbal Tilt" field and handle actions
    gimbal_tilt = metadata_dict.get("Gimbal Tilt")
    if gimbal_tilt:
        print(f"Gimbal Tilt: {gimbal_tilt}")
        handle_gimbal_tilt(gimbal_tilt)
    else:
        print("Gimbal Tilt not found in metadata.")

# Define default arguments for the DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'tiff_control',
    default_args=default_args,
    description='DAG que controla los tiff llegados a Airflow',
    schedule_interval=None,
    catchup=False
)

# Define the tasks
print_message_task = PythonOperator(
    task_id='print_message',
    python_callable=lambda **context: print("El mensaje se ha recibido correctamente"),
    provide_context=True,
    dag=dag,
)

run_docker_task = SSHOperator(
    task_id='run_docker',
    ssh_conn_id='ssh_docker',
    command='docker run --rm -v /servicios/exiftool:/images --name exiftool-container-new exiftool-image -config /images/example1.1.0_missionId.txt -u /images/img-20230924140747117-ter.tiff',
    dag=dag,
    do_xcom_push=True,
)

process_metadata_task = PythonOperator(
    task_id='process_metadata',
    python_callable=process_metadata,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
print_message_task >> run_docker_task >> process_metadata_task