import io
import json
import tempfile
import uuid
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import boto3
from botocore.client import Config
from airflow.hooks.base_hook import BaseHook
from airflow.providers.sftp.hooks.sftp import SFTPHook

def download_tiff_from_sftp():
    sftp_hook = SFTPHook(ftp_conn_id='your_sftp_connection_id')
    
    remote_file_path = "/remote/path/to/your/filename.tiff"
    local_file_path = os.path.join(tempfile.gettempdir(), "downloaded_file.tiff")
    
    print(f"Descargando archivo desde SFTP: {remote_file_path} a {local_file_path}")
    
    sftp_hook.retrieve_file(remote_file_path, local_file_path)
    
    return local_file_path

def create_minio_client():
    print("Obteniendo la conexión de MinIO desde Airflow...")
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)

    print("Conexión de MinIO obtenida:", json.dumps(extra, indent=2))

    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

    return s3_client

def upload_tiff_to_minio(tiff_file_path):
    s3_client = create_minio_client()

    tiff_file_name = str(uuid.uuid4()) + ".tiff"
    print(f"Generado UUID para el archivo TIFF: {tiff_file_name}")

    bucket_name = "temp"

    try:
        print(f"Verificando si el bucket '{bucket_name}' existe en MinIO...")
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"El bucket '{bucket_name}' ya existe.")
    except s3_client.exceptions.NoSuchBucket:
        print(f"El bucket '{bucket_name}' no existe. Creándolo ahora...")
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' creado exitosamente.")

    print(f"Subiendo el archivo {tiff_file_name} a MinIO en el bucket '{bucket_name}'...")
    s3_client.upload_file(tiff_file_path, bucket_name, tiff_file_name)
    print(f"Archivo {tiff_file_name} subido exitosamente a MinIO.")

    tiff_file_url = f"{s3_client.meta.endpoint_url}/{bucket_name}/{tiff_file_name}"
    print(f"URL del archivo TIFF subido: {tiff_file_url}")

    return tiff_file_url

def process_and_upload_tiff():
    createdJob = {
        "inputData": {
            # Otros datos que ya podrían estar
        }
    }

    print("Añadiendo datos adicionales al inputData...")

    createdJob["inputData"]["dir_output"] = "/path/to/output/directory"
    createdJob["inputData"]["ar_incendios"] = "/path/to/incendios.csv"
    createdJob["inputData"]["url_search_fire"] = "https://pre.atcservices.cirpas.gal/rest/FireService/searchByIntersection"
    createdJob["inputData"]["url_fireperimeter_service"] = "https://pre.atcservices.cirpas.gal/rest/FireAlgorithm_FirePerimeterService/getByFire?id="
    createdJob["inputData"]["user"] = "jose.blanco"
    createdJob["inputData"]["password"] = "babcock"

    print("Datos de entrada modificados:", json.dumps(createdJob, indent=2))

    # Descargar el archivo TIFF desde SFTP
    tiff_file_path = download_tiff_from_sftp()
    print(f"Archivo TIFF descargado: {tiff_file_path}")

    # Subir el archivo TIFF a MinIO
    tiff_file_url = upload_tiff_to_minio(tiff_file_path)

    notification = {
        "url": tiff_file_url
    }

    print("Notificación a Ignis:", json.dumps(notification, indent=2))

    createdJob["inputData"]["dir_output"] = tiff_file_url

    print("Resultado final de createdJob:", json.dumps(createdJob, indent=2))

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
    'heat_map',
    default_args=default_args,
    description='Un DAG para recopilar información sobre los mapas de calor en MinIO',
    schedule_interval=None,
    catchup=False,
)

heat_map_task = PythonOperator(
    task_id='heat_map',
    python_callable=process_and_upload_tiff,
    dag=dag,
)
