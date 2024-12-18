import os
import json
import uuid
import ffmpeg
import tempfile
from datetime import datetime, timedelta, timezone
import boto3
from botocore.client import Config
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def download_convert_upload_video(**kwargs):
    """ Descarga un video .ts desde MinIO, lo convierte a .mp4 con ffmpeg y lo sube de nuevo a MinIO. """
    video_key = kwargs['dag_run'].conf.get('video_key')
    if not video_key:
        raise ValueError("No se ha proporcionado una clave de video .ts.")

    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )
    bucket_name = 'missions'
    resource_id = str(uuid.uuid4())

    temp_dir = tempfile.mkdtemp()
    input_file_path = os.path.join(temp_dir, "input.ts")
    output_file_path = os.path.join(temp_dir, "output.mp4")

    try:
        s3_client.download_file(bucket_name, video_key, input_file_path)
        ffmpeg.input(input_file_path).output(output_file_path, codec="libx264", audio_bitrate="128k").run()
        mp4_key = video_key.replace(".ts", ".mp4")
        s3_client.upload_file(output_file_path, bucket_name, mp4_key)
        insert_job(resource_id, mp4_key)

        # Pasar resource_id a la siguiente tarea
        kwargs['ti'].xcom_push(key='resource_id', value=resource_id)

    finally:
        os.remove(input_file_path)
        os.remove(output_file_path)
        os.rmdir(temp_dir)


def insert_job(resource_id, mp4_key):
    db_conn = BaseHook.get_connection('biobd')
    connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()

    input_data = json.dumps({"resource_id": resource_id, "file": mp4_key})
    time_now = datetime.now(timezone.utc)

    try:
        query = text("""
            INSERT INTO public.jobs (job, input_data, date, status)
            VALUES (:job_name, :data, :date, :status);
        """)
        session.execute(query, {
            'job_name': "convert-ts-to-mp4",
            'data': input_data,
            'date': time_now,
            'status': "QUEUED"
        })
        session.commit()
    finally:
        session.close()


def notify_mission_update(**kwargs):
    resource_id = kwargs['ti'].xcom_pull(task_ids='download_convert_upload_video', key='resource_id')
    if not resource_id:
        raise ValueError("No se encontró el resource_id.")

    db_conn = BaseHook.get_connection('biobd')
    connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()

    data_json = json.dumps({"action": "reloadMission", "resource_id": resource_id})
    time_now = datetime.now(timezone.utc)

    try:
        query = text("""
            INSERT INTO public.notifications (destination, data, date, status)
            VALUES (:destination, :data, :date, NULL);
        """)
        session.execute(query, {
            'destination': "inspection",
            'data': data_json,
            'date': time_now
        })
        session.commit()
    finally:
        session.close()


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
    description='DAG que convierte videos .ts a .mp4, sube a MinIO y notifica',
    schedule_interval=None,
    catchup=False,
)

convert_task = PythonOperator(
    task_id='download_convert_upload_video',
    python_callable=download_convert_upload_video,
    dag=dag,
)

notify_task = PythonOperator(
    task_id='notify_mission_update',
    python_callable=notify_mission_update,
    dag=dag,
)

convert_task >> notify_task
