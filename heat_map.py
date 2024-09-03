from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import tempfile
import uuid
import boto3
from botocore.client import Config
from airflow.hooks.base_hook import BaseHook
import os
import requests

# Función principal que procesa los datos de entrada y realiza las tareas solicitadas
def process_heatmap_data(**context):

    # Simulación de lectura desde la tabla JOBS (input_data)
    
    input_data = {
        "temp_tiff_path": "AIRFLOW/dags/f496d404-85d9-4c66-9b16-1e5fd9da85b9.tif"  # Ruta al TIFF de Agustín
    }

    # Log para verificar que los datos están completos
    print("Datos completos de entrada para heatmap-incendio:")
    print(json.dumps(input_data, indent=4))

    # Simulación de obtener un archivo TIFF de una carpeta temporal (usando el de Agustín)
    temp_tiff_path = input_data['temp_tiff_path']

    # Subir el archivo TIFF a MinIO
    try:
        connection = BaseHook.get_connection('minio_conn')
        extra = json.loads(connection.extra)
        s3_client = boto3.client(
            's3',
            endpoint_url=extra['endpoint_url'],
            aws_access_key_id=extra['aws_access_key_id'],
            aws_secret_access_key=extra['aws_secret_access_key'],
            config=Config(signature_version='s3v4')
        )

        bucket_name = 'temp'
        tiff_key = f"{uuid.uuid4()}.tiff"

        s3_client.upload_file(temp_tiff_path, bucket_name, tiff_key)
        tiff_url = f"{extra['endpoint_url']}/{bucket_name}/{tiff_key}"
        print(f"Archivo TIFF subido correctamente a MinIO. URL: {tiff_url}")
        
    except Exception as e:
        print(f"Error al subir el TIFF a MinIO: {str(e)}")
        return
    
    # Enviar notificación a "ignis" con la URL del TIFF
    notification_payload = {
        "urlTiff": tiff_url
    }

    try:
        response = requests.post("https://ignis.endpoint.url/notify", json=notification_payload)
        if response.status_code == 200:
            print("Notificación enviada correctamente a 'ignis'.")
        else:
            print(f"Error al enviar notificación a 'ignis': {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error al enviar notificación a 'ignis': {str(e)}")

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'heatmap_incendio_process',
    default_args=default_args,
    description='DAG para procesar datos de heatmap-incendio y enviar TIFF a MinIO',
    schedule_interval=None,
    catchup=False
)

process_heatmap_task = PythonOperator(
    task_id='process_heatmap_data',
    python_callable=process_heatmap_data,
    provide_context=True,
    dag=dag,
)

process_heatmap_task
