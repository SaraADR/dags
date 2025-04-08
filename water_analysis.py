import base64
import json
import uuid
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
import io
from sqlalchemy import  text
from dag_utils import get_db_session, get_minio_client


# Función para procesar archivos extraídos
def process_extracted_files(**kwargs):
    archivos = kwargs['dag_run'].conf.get('otros', [])
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

    uuid_key = uuid.uuid4()
    print(f"UUID generado para almacenamiento: {uuid_key}")  


    for archivo in archivos:
        archivo_file_name = archivos['file_name']
        archivo_content = base64.b64decode(archivos['content'])

        s3_client = get_minio_client()


        bucket_name = 'missions'
        archivo_key = f"{id_mission}/{uuid_key}/{archivo_file_name}"


        s3_client.put_object(
            Bucket=bucket_name,
            Key=archivo_key,
            Body=io.BytesIO(archivo_content),
        )
        print(f'{archivo_file_name} subido correctamente a MinIO.')

    json_str = json.dumps(json_content).encode('utf-8')
    json_key = f"{id_mission}/{uuid_key}/algorithm_result.json"

    s3_client.put_object(
        Bucket='missions',
        Key=json_key,
        Body=io.BytesIO(json_str),
        ContentType='application/json'
    )
    print(f'Archivo JSON subido correctamente a MinIO.')

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
            session = get_db_session()

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
    'waterAnalysis',
    default_args=default_args,
    description='DAG que procesa analisis de aguas',
    schedule_interval=None,
    catchup=False,
)
process_extracted_files_task = PythonOperator(
    task_id='process_extracted_files_task',
    python_callable=process_extracted_files,
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
process_extracted_files_task >> generate_notify