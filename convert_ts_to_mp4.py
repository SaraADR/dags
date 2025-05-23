import os
import json
import tempfile
from moviepy import VideoFileClip
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
import boto3
from botocore.client import Config
from dag_utils import get_db_session, get_minio_client

def convert_ts_to_mp4(**kwargs):
    """
    Convierte un archivo .ts a .mp4 usando MoviePy y actualiza el estado del trabajo en la base de datos.
    """
    print("Iniciando conversión de archivo .ts a .mp4...")

    # Leer datos desde `dag_run.conf`
    conf = kwargs['dag_run'].conf
    print(f"Conf recibido: {conf}")

    if not conf:
        raise ValueError("No se proporcionaron datos en `dag_run.conf`.")

    # Extraer datos desde el objeto anidado `message`
    message = conf.get('message', {})
    job_id = message.get('id')
    resource_id = json.loads(message.get('input_data', '{}')).get('resource_id')  # Decodificar input_data
    from_user = message.get('from_user')

    # Debugging detallado
    print(f"Job ID: {job_id}")
    print(f"Resource ID: {resource_id}")
    print(f"From User: {from_user}")

    if not resource_id or not job_id:
        raise ValueError("Faltan datos necesarios: `resource_id` o `job_id`.")

    # Configuración del bucket y directorios
    print("Configurando conexión con MinIO...")
    s3_client = get_minio_client()


    # Nuevos valores para el bucket y directorios
    bucket_name = 'tmp'  # Bucket donde está el archivo .ts
    folder_prefix = f'metadatos/12c23bc3-8738-4d07-bc29-fdcd82621f62/'  # Prefijo en el bucket, incluye subdirectorio
    local_directory = 'tmp'  # Directorio local para trabajo temporal
    ts_key = f"{folder_prefix}{resource_id}.ts"  # Ruta completa del archivo TS
    mp4_key = f"{folder_prefix}{resource_id}.mp4"  # Ruta completa del archivo MP4
    print(f"Bucket Name: {bucket_name}")
    print(f"Archivo TS en MinIO: {ts_key}")
    print(f"Archivo MP4 en MinIO: {mp4_key}")

    # Crear directorios temporales
    print("Creando directorio temporal para la conversión...")
    temp_dir = tempfile.mkdtemp()
    ts_path = os.path.join(temp_dir, "input.ts")
    mp4_path = os.path.join(temp_dir, "output.mp4")
    print(f"Directorio temporal creado: {temp_dir}")
    print(f"Ruta archivo TS local: {ts_path}")
    print(f"Ruta archivo MP4 local: {mp4_path}")

    try:
        # Descargar archivo .ts desde MinIO
        print(f"Descargando archivo desde MinIO: {ts_key}...")
        s3_client.download_file(bucket_name, ts_key, ts_path)
        print(f"Archivo descargado correctamente: {ts_path}")

        # Convertir archivo a .mp4 con MoviePy
        print(f"Iniciando conversión de {ts_path} a {mp4_path}...")
        video = VideoFileClip(ts_path)
        video.write_videofile(mp4_path, codec="libx264", audio_codec="aac")
        print(f"Conversión completada. Archivo convertido: {mp4_path}")

        # Subir archivo convertido a MinIO
        print(f"Subiendo archivo convertido a MinIO: {mp4_key}...")
        s3_client.upload_file(mp4_path, bucket_name, mp4_key)
        print(f"Archivo subido correctamente: {mp4_key}")

        # Actualizar estado del trabajo a FINISHED
        print(f"Actualizando estado del trabajo {job_id} a FINISHED...")
        update_job_status(job_id, "FINISHED", {"resource_id": resource_id, "file": mp4_key})
        print(f"Estado actualizado exitosamente para Job ID: {job_id}")

    except Exception as e:
        print(f"Error durante la conversión: {e}")
        print(f"Actualizando estado del trabajo {job_id} a ERROR...")
        update_job_status(job_id, "ERROR", {"error": str(e)})
        print(f"Estado actualizado a ERROR para Job ID: {job_id}")
        raise

    finally:
        print(f"Limpiando directorios temporales en: {temp_dir}...")
        os.remove(ts_path)
        os.remove(mp4_path)
        os.rmdir(temp_dir)
        print(f"Archivos temporales eliminados.")

def update_job_status(job_id, status, output_data=None):
    """
    Actualiza el estado del trabajo en la tabla `jobs`.
    """
    print(f"Iniciando actualización de estado para Job ID: {job_id}...")
    session = get_db_session()
    engine = session.get_bind()
  
    with engine.connect() as connection:
        query = text("""
            UPDATE public.jobs
            SET status = :status, output_data = :output_data, execution_date = :execution_date
            WHERE id = :job_id;
        """)
        connection.execute(query, {
            'status': status,
            'output_data': json.dumps(output_data) if output_data else None,
            'execution_date': datetime.now(timezone.utc),
            'job_id': job_id
        })
        print(f"Estado del trabajo {job_id} actualizado a {status}.")


# Configuración por defecto para el DAG
default_args = {
    'owner': 'converter',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
dag = DAG(
    'convert_ts_to_mp4_dag',
    default_args=default_args,
    description='Convierte archivos .ts a .mp4 desde MinIO y actualiza el estado del trabajo',
    schedule_interval=None,
    catchup=False,
)

# Tarea: Conversión de archivo
convert_task = PythonOperator(
    task_id='convert_ts_to_mp4_task',
    python_callable=convert_ts_to_mp4,
    provide_context=True,
    dag=dag,
)