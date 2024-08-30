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
from test_busqueda_minio import retrieve_from_minio

def create_minio_client():
    # Obtener la conexión de MinIO desde Airflow
    print("Obteniendo la conexión de MinIO desde Airflow...")
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)

    print("Conexión de MinIO obtenida:", json.dumps(extra, indent=2))

    # Crear el cliente de MinIO/S3 con las credenciales y configuración necesarias
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

    return s3_client

def upload_tiff_to_minio(tiff_file_path):
    # Crear cliente de MinIO
    s3_client = create_minio_client()

    # Generar un UUID para el archivo en MinIO
    tiff_file_name = str(uuid.uuid4()) + ".tiff"
    print(f"Generado UUID para el archivo TIFF: {tiff_file_name}")

    # Nombre del bucket
    bucket_name = "temp"

    # Crear el bucket si no existe
    try:
        print(f"Verificando si el bucket '{bucket_name}' existe en MinIO...")
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"El bucket '{bucket_name}' ya existe.")
    except s3_client.exceptions.NoSuchBucket:
        print(f"El bucket '{bucket_name}' no existe. Creándolo ahora...")
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' creado exitosamente.")

    # Subir el archivo TIFF a MinIO
    print(f"Subiendo el archivo {tiff_file_name} a MinIO en el bucket '{bucket_name}'...")
    s3_client.upload_file(tiff_file_path, bucket_name, tiff_file_name)
    print(f"Archivo {tiff_file_name} subido exitosamente a MinIO.")

    # Generar la URL del archivo TIFF subido
    tiff_file_url = f"{s3_client.meta.endpoint_url}/{bucket_name}/{tiff_file_name}"
    print(f"URL del archivo TIFF subido: {tiff_file_url}")

    return tiff_file_url

def process_and_upload_tiff():
    # Simulación de datos de entrada del job de heatmap-incendio
    createdJob = {
        "inputData": {
            # Otros datos que ya podrían estar
        }
    }

    print("Añadiendo datos adicionales al inputData...")

    # Añadir los 6 datos adicionales al inputData
    createdJob["inputData"]["dir_output"] = "/path/to/output/directory"
    createdJob["inputData"]["ar_incendios"] = "/path/to/incendios.csv"
    createdJob["inputData"]["url_search_fire"] = "https://pre.atcservices.cirpas.gal/rest/FireService/searchByIntersection"
    createdJob["inputData"]["url_fireperimeter_service"] = "https://pre.atcservices.cirpas.gal/rest/FireAlgorithm_FirePerimeterService/getByFire?id="
    createdJob["inputData"]["user"] = "jose.blanco"
    createdJob["inputData"]["password"] = "babcock"

    # Verificación de los datos añadidos
    print("Datos de entrada modificados:", json.dumps(createdJob, indent=2))

    # Ruta del archivo TIFF en la carpeta temporal
    tiff_file_path = "dags/recursos/433e21e9-d5ae-4429-b8d0-2e3e36f3f299.tif"  # Cambia este path al correcto
    print(f"Archivo TIFF a subir: {tiff_file_path}")

    # Subir el archivo TIFF a MinIO
    tiff_file_url = upload_tiff_to_minio(tiff_file_path)

    # Enviar notificación a "ignis" con la URL del archivo TIFF
    notification = {
        "url": tiff_file_url
    }

    # Console log del JSON de notificación
    print("Notificación a Ignis:", json.dumps(notification, indent=2))

    # Actualizar dir_output en createdJob con la URL del TIFF
    createdJob["inputData"]["dir_output"] = tiff_file_url

    # Resultado final de createdJob
    print("Resultado final de createdJob:", json.dumps(createdJob, indent=2))

# Configuración de los default_args para el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configuración del DAG para que se ejecute solo una vez
dag = DAG(
    'heat_map',
    default_args=default_args,
    description='Un DAG para recopilar información sobre los mapas de calor en MinIO',
    schedule_interval=None,  # No se programa para ejecución automática
    catchup=False,  # Evitar ejecución múltiple si la fecha de inicio es en el pasado
)

# Definición de la tarea en el DAG
heat_map_task = PythonOperator(
    task_id='heat_map',
    python_callable=process_and_upload_tiff,
    dag=dag,
)
