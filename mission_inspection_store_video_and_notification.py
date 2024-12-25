import base64
import json
import uuid
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
import boto3
from botocore.client import Config
from airflow.hooks.base import BaseHook
import io
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from moviepy import VideoFileClip
import tempfile
import os

# Función para procesar archivos extraídos
def process_extracted_files(**kwargs):
    video = kwargs['dag_run'].conf.get('otros', [])
    json_content = kwargs['dag_run'].conf.get('json')

    if not json_content:
        print("Ha habido un error con el traspaso de los documentos")
        return

    print("Archivos para procesar preparados")

    id_mission = None
    for metadata in json_content['metadata']:
        if metadata['name'] == 'MissionID':
            id_mission = metadata['value']
            break

    print(f"MissionID: {id_mission}")

    try:
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

        query = text("""
            SELECT *
            FROM missions.mss_mission_inspection
            WHERE mission_id = :search_id
        """)
        result = session.execute(query, {'search_id': id_mission})
        row = result.fetchone()
        mission_inspection_id = row[0] if row else None

    except Exception as e:
        session.rollback()
        print(f"Error durante la búsqueda del mission_inspection: {str(e)}")
    finally:
        session.close()

    print(f"Mission Inspection ID: {mission_inspection_id}")

    uuid_key = uuid.uuid4()
    print(f"UUID generado para almacenamiento: {uuid_key}")  


    for videos in video:
        video_file_name = videos['file_name']
        video_content = base64.b64decode(videos['content'])

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
        video_key = str(uuid_key) + '/' + video_file_name

        s3_client.put_object(
            Bucket=bucket_name,
            Key=video_key,
            Body=io.BytesIO(video_content),
        )
        print(f'{video_file_name} subido correctamente a MinIO.')

    json_str = json.dumps(json_content).encode('utf-8')
    json_key = str(uuid_key) + '/' + 'algorithm_result.json'

    s3_client.put_object(
        Bucket='missions',
        Key=json_key,
        Body=io.BytesIO(json_str),
        ContentType='application/json'
    )
    print(f'Archivo JSON subido correctamente a MinIO.')

    return uuid_key

    # kwargs['ti'].xcom_push(key='video_uuid', value=str(uuid_key))
    # kwargs['ti'].xcom_push(key='mission_inspection_id', value=mission_inspection_id)

# Función para convertir archivos TS a MP4
def convert_ts_files(**kwargs):
    videos = kwargs['dag_run'].conf.get('otros', [])
    converted_files = []

    ti = kwargs['ti']
    generated_uuid = ti.xcom_pull(task_ids='process_extracted_files_task')
    print(f"UUID recibido en convert_ts_files: {generated_uuid}")

    for video in videos:
        file_name = video['file_name']
        file_content = base64.b64decode(video['content'])
        print("El nombre del video es " + file_name)

    if file_name.endswith('.ts'):
        print(f"Archivo {file_name} es .ts. Iniciando conversión a .mp4...")
    with tempfile.TemporaryDirectory() as temp_dir:
        ts_path = os.path.join(temp_dir, "input.ts")
        mp4_path = os.path.join(temp_dir, "output.mp4")

        # Guardar contenido .ts temporalmente
        with open(ts_path, 'wb') as f:
            f.write(file_content)

        # Convertir a .mp4
        video_clip = VideoFileClip(ts_path)
        video_clip.write_videofile(mp4_path, codec="libx264", audio_codec="aac")

        # Leer el archivo convertido y subir a MinIO
        bucket_name = 'missions'
        mp4_file_name = file_name.replace('.ts', '.mp4')
        with open(mp4_path, 'rb') as f:
            connection = BaseHook.get_connection('minio_conn')
            extra = json.loads(connection.extra)
            s3_client = boto3.client(
                's3',
                endpoint_url=extra['endpoint_url'],
                aws_access_key_id=extra['aws_access_key_id'],
                aws_secret_access_key=extra['aws_secret_access_key'],
                config=Config(signature_version='s3v4')
            )

            s3_client.put_object(
                Bucket=bucket_name,
                Key=f"{str(generated_uuid)}/{mp4_file_name}",
                Body=f.read(),
                ContentType='video/mp4'
            )
        print(f"Archivo convertido y subido a MinIO como {mp4_file_name}.")


# Función para generar notificación
def generate_notify_job(**context):
    json_content = context['dag_run'].conf.get('json')
    id_mission = None
    for metadata in json_content['metadata']:
        if metadata['name'] == 'MissionID':
            id_mission = metadata['value']
            break

    if id_mission:
        try:
            db_conn = BaseHook.get_connection('biobd')
            connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
            engine = create_engine(connection_string)
            Session = sessionmaker(bind=engine)
            session = Session()

            data_json = json.dumps({
                "to": "all_users",
                "actions": [{
                    "type": "reloadMission",
                    "data": {"missionId": id_mission}
                }]
            })
            time = datetime.now().replace(tzinfo=timezone.utc)

            query = text("""
                INSERT INTO public.notifications
                (destination, "data", "date", status)
                VALUES (:destination, :data, :date, NULL);
            """)
            session.execute(query, {
                'destination': 'inspection',
                'data': data_json,
                'date': time
            })
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error durante la inserción de la notificación: {str(e)}")
        finally:
            session.close()

# Definición del DAG
default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mission_inspection_store_video_and_notification',
    default_args=default_args,
    description='DAG que procesa archivos extraídos, convierte videos si es necesario, y genera notificaciones.',
    schedule_interval=None,
    catchup=False,
)

process_extracted_files_task = PythonOperator(
    task_id='process_extracted_files_task',
    python_callable=process_extracted_files,
    provide_context=True,
    dag=dag,
)

convert_videos_task = PythonOperator(
    task_id='convert_videos_task',
    python_callable=convert_ts_files,
    provide_context=True,
    dag=dag,
)

generate_notify = PythonOperator(
    task_id='generate_notify_job',
    python_callable=generate_notify_job,
    provide_context=True,
    dag=dag,
)

# Flujo del DAG
process_extracted_files_task >> convert_videos_task >> generate_notify
