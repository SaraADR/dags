import os
import json
import subprocess
import tempfile
import boto3
from botocore.client import Config
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta

# Función principal para la conversión
def convert_ts_to_mp4(**kwargs):
    # Obtener la clave del video desde dag_run.conf
    video_key = kwargs['dag_run'].conf.get('video_key')
    if not video_key:
        raise ValueError("No se ha proporcionado una clave de video .ts")

    # Configuración de MinIO
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )
    
    bucket_name = 'missions'  # Bucket donde están los archivos

    # Directorios temporales
    temp_dir = tempfile.mkdtemp()
    input_file_path = os.path.join(temp_dir, "input.ts")
    output_file_path = os.path.join(temp_dir, "output.mp4")

    try:
        # Descargar el archivo .ts desde MinIO
        print(f"Descargando {video_key} desde MinIO...")
        s3_client.download_file(bucket_name, video_key, input_file_path)
        print("Descarga completada.")

        # Convertir el archivo usando FFmpeg
        print("Iniciando conversión a .mp4...")
        ffmpeg_command = f"ffmpeg -i {input_file_path} -c:v libx264 -c:a aac {output_file_path}"
        subprocess.run(ffmpeg_command, shell=True, check=True)
        print("Conversión completada.")

        # Subir el archivo convertido a MinIO
        mp4_key = video_key.replace(".ts", ".mp4")
        print(f"Subiendo {mp4_key} a MinIO...")
        s3_client.upload_file(output_file_path, bucket_name, mp4_key)
        print(f"Archivo {mp4_key} subido exitosamente.")

    except Exception as e:
        print(f"Error durante la conversión: {str(e)}")
        raise
    finally:
        # Limpieza de archivos temporales
        os.remove(input_file_path)
        os.remove(output_file_path)
        os.rmdir(temp_dir)
        print("Archivos temporales eliminados.")

# Configuración del DAG
default_args = {
    'owner': 'converter',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 17),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'convert_ts_to_mp4_dag',
    default_args=default_args,
    description='DAG para convertir videos .ts a .mp4 en MinIO',
    schedule_interval=None,  # Se ejecuta bajo demanda
    catchup=False,
)

# Tarea de conversión
convert_video_task = PythonOperator(
    task_id='convert_ts_to_mp4',
    python_callable=convert_ts_to_mp4,
    provide_context=True,
    dag=dag,
)
