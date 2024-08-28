import json
import uuid
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3
from botocore.client import Config
from airflow.hooks.base_hook import BaseHook
import io
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.orm import sessionmaker

def process_extracted_files(**kwargs):
    # Obtén los archivos extraídos que se pasan como "conf"
    video = kwargs['dag_run'].conf.get('video', [])
    imagen = kwargs['dag_run'].conf.get('imagen', [])
    json_content = kwargs['dag_run'].conf.get('json_content', [])

    
    if not json_content:
        print("Ha habido un error con el traspaso de los documentos")
        return

    print("Archivos para procesar preparados")

    id_mission = json_content.get('metadata.MissionID')

    
    #Comprobamos que no exista un mission_inspection ya creado para esa mision
    try:
        # Conexión a la base de datos usando las credenciales almacenadas en Airflow
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

        query = text("""
            SELECT *
            FROM mission.mss_mission_inspection
            WHERE mission_id = :search_id
        """)
        result = session.execute(query, {'search_id': id_mission})
        row = result.fetchone()

        if row is None:
            print("El ID no está presente en la tabla mission.mss_mission_inspection")
            # Lo creamos

            # query = text("""
            # INSERT INTO missions.mss_mission_inspection
            # (mission_id, creationtimestamp, capturedate, operator_id, longitude, infrastructure_id)
            # VALUES(:mission_id, :creation_timestamp, :creation_timestamp, 1, 0.0, 6)
            #          """)


    except Exception as e:
        session.rollback()
        print(f"Error durante la busqueda del mission_inspection: {str(e)}")



    for videos in video:
        connection = BaseHook.get_connection('minio_conn')
        extra = json.loads(connection.extra)
        s3_client = boto3.client(
            's3',
            endpoint_url=extra['endpoint_url'],
            aws_access_key_id=extra['aws_access_key_id'],
            aws_secret_access_key=extra['aws_secret_access_key'],
            config=Config(signature_version='s3v4')
        )

        bucket_name = 'avincis-test'  
        video_key = str(uuid.uuid4())

        # Subir el archivo a MinIO
        s3_client.put_object(
            Bucket=bucket_name,
            Key=video_key,
            Body=io.BytesIO(videos),
        )
        print(f'{video_key} subido correctamente a MinIO.')



        #Creamos el mss_inspection_video

        query = text("""
        INSERT INTO missions.mss_inspection_video
        (mission_inspection_id, resource_id, reviewed)
        VALUES( :id_video, :id_resource::uuid, false);
        """)
        result = session.execute(query, {'id_resource': video_key, 'id_video': row['id']})
        row = result.fetchone()






  



    





default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'video',
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


process_extracted_files_task 
