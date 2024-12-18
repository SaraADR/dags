import os
import json
import tempfile
import ffmpeg
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
import boto3
from botocore.client import Config

# Función principal: Conversión de .ts a .mp4
def convert_ts_to_mp4(**kwargs):
    """
    Convierte un archivo .ts a .mp4 usando ffmpeg y actualiza el estado del trabajo en la base de datos.
    """
    # Leer datos desde `dag_run.conf`
    conf = kwargs['dag_run'].conf
    if not conf:
        raise ValueError("No se proporcionaron datos en `dag_run.conf`.")

    # Extraer datos necesarios
    job_id = conf.get('id')
    resource_id = conf.get('input_data', {}).get('resource_id')
    from_user = conf.get('from_user')

    if not resource_id or not job_id:
        raise ValueError("Faltan datos necesarios: `resource_id` o `job_id`.")

    # Configurar MinIO
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

    # Configuración del bucket y directorios
    bucket_name = 'missions'
    ts_key = f"{resource_id}.ts"
    mp4_key = f"{resource_id}.mp4"

    # Crear directorios temporales
    temp_dir = tempfile.mkdtemp()
    ts_path = os.path.join(temp_dir, "input.ts")
    mp4_path = os.path.join(temp_dir, "output.mp4")

    try:
        # Descargar archivo .ts desde MinIO
        print(f"Descargando {ts_key} desde el bucket {bucket_name}...")
        s3_client.download_file(bucket_name, ts_key, ts_path)

        # Convertir archivo a .mp4 con ffmpeg
        print(f"Convirtiendo {ts_key} a {mp4_key}...")
        ffmpeg.input(ts_path).output(mp4_path, codec="libx264", audio_bitrate="128k").run()

        # Subir archivo convertido a MinIO
        print(f"Subiendo {mp4_key} al bucket {bucket_name}...")
        s3_client.upload_file(mp4_path, bucket_name, mp4_key)

        # Actualizar estado del trabajo a FINISHED
        update_job_status(job_id, "FINISHED", {"resource_id": resource_id, "file": mp4_key})
        print(f"Conversión completada y estado actualizado para Job ID: {job_id}.")

    except Exception as e:
        print(f"Error durante la conversión: {e}")
        update_job_status(job_id, "FAILED", {"error": str(e)})
        raise

    finally:
        # Eliminar archivos temporales
        os.remove(ts_path)
        os.remove(mp4_path)
        os.rmdir(temp_dir)


# Función para actualizar el estado del trabajo en la base de datos
def update_job_status(job_id, status, output_data=None):
    """
    Actualiza el estado del trabajo en la tabla `jobs`.
    """
    db_conn = BaseHook.get_connection('biobd')
    connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
    engine = create_engine(connection_string)
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
        print(f"Estado actualizado para Job ID {job_id} con estado {status}.")


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
