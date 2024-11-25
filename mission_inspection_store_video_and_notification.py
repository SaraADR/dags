import base64
import json
import uuid
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
import boto3
from botocore.client import Config
from airflow.hooks.base_hook import BaseHook
import io
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.orm import sessionmaker

def process_extracted_files(**kwargs):
    # Obtenemos los archivos extraídos que se pasan como "conf"
    video = kwargs['dag_run'].conf.get('otros', [])
    json_content = kwargs['dag_run'].conf.get('json')

    if not json_content:
        print("Ha habido un error con el traspaso de los documentos")
        return

    print("Archivos para procesar preparados")

    #Accedemos al missionID para poder buscar si ya existe
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
        if row is not None:
            mission_inspection_id = row[0]  
        else:
            mission_inspection_id = None
            print("El ID no está presente en la tabla mission.mss_mission_inspection")

    except Exception as e:
        session.rollback()
        print(f"Error durante la busqueda del mission_inspection: {str(e)}")
        


    print(f"row: {row}")

    uuid_key= uuid.uuid4()
    #Subimos todos los videos a la carpeta de minIo
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
        video_key = str(uuid_key) +'/' + video_file_name

        # Subir el archivo a MinIO
        s3_client.put_object(
            Bucket=bucket_name,
            Key=video_key,
            Body=io.BytesIO(video_content),
        )
        print(f'{video_file_name} subido correctamente a MinIO.')

    #Subimos el archivo JSON
    json_str = json.dumps(json_content).encode('utf-8')
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
    json_key = str(uuid_key) +'/' + 'algorithm_result.json'

    # Subir el archivo a MinIO
    s3_client.put_object(
        Bucket=bucket_name,
        Key=json_key,
        Body=io.BytesIO(json_str),
        ContentType='application/json'
    )
    print(f'{video_file_name} subido correctamente a MinIO.')




    try:

        id_resource_uuid = uuid.UUID(video_key.split('/')[0])

        query = text("""
        INSERT INTO missions.mss_inspection_video 
        (mission_inspection_id, resource_id, reviewed)
        VALUES (:id_video, :id_resource, false)
        """)
        session.execute(query, {'id_resource': id_resource_uuid, 'id_video': mission_inspection_id})
        session.commit()
        print(f"Video {video_key} registrado en la inspección {mission_inspection_id}")
    except Exception as e:
        session.rollback()
        print(f"Error al insertar video en mss_inspection_video: {str(e)}")


    finally:
        session.close()
        print("Conexión a la base de datos cerrada correctamente")

  
def get_idmission(data):
    for metadata in data['metadata']:
        if metadata['name'] == 'MissionID':
            return metadata['value']
    return None

def generate_notify_job(**context):
    json_content = context['dag_run'].conf.get('json')
 
    if not json_content:
        print("Ha habido un error con el traspaso de los documentos")
        return

    #Accedemos al missionID para poder buscar si ya existe
    id_mission = get_idmission(json_content)
    print(f"MissionID: {id_mission}")


    if id_mission is not None:
        #Añadimos notificacion
        try:
            db_conn = BaseHook.get_connection('biobd')
            connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
            engine = create_engine(connection_string)
            Session = sessionmaker(bind=engine)
            session = Session()

            data_json = json.dumps({
                "to":"all_users",
                "actions":[{
                    "type":"reloadMission",
                    "data":{
                        "missionId":id_mission
                    }
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
    description='DAG que procesa archivos extraídos del ZIP, imprime JSON, guarda videos en MinIO y retorna IDs',
    schedule_interval=None,
    catchup=False,
)

process_extracted_files_task = PythonOperator(
    task_id='process_extracted_files_task',
    python_callable=process_extracted_files,
    provide_context=True,
    dag=dag,
)

#Generar notificación de vuelta
generate_notify = PythonOperator(
    task_id='generate_notify_job',
    python_callable=generate_notify_job,
    provide_context=True,
    dag=dag,
)




process_extracted_files_task >> generate_notify
