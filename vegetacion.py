import base64
import json
import os
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
    # Obtenemos los archivos extraídos que se pasan como "conf"
    video = kwargs['dag_run'].conf.get('videos', [])
    imagen = kwargs['dag_run'].conf.get('imagen', [])
    otros = kwargs['dag_run'].conf.get('otros', [])
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

    print(f"otros: {otros}")
    
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
            # Lo creamos?

    except Exception as e:
        session.rollback()
        print(f"Error durante la busqueda del mission_inspection: {str(e)}")
        
    print(f"row: {row}")



    # #Subimos el archivo JSON
    # json_str = json.dumps(json_content).encode('utf-8')
    # connection = BaseHook.get_connection('minio_conn')
    # extra = json.loads(connection.extra)
    # s3_client = boto3.client(
    #     's3',
    #     endpoint_url=extra['endpoint_url'],
    #     aws_access_key_id=extra['aws_access_key_id'],
    #     aws_secret_access_key=extra['aws_secret_access_key'],
    #     config=Config(signature_version='s3v4')
    # )

    # bucket_name = 'missions'  
    # uuid_key= uuid.uuid4()
    # json_key = str(uuid_key) +'/' + 'algorithm_result.json'

    # # Subir el archivo a MinIO
    # s3_client.put_object(
    #     Bucket=bucket_name,
    #     Key=json_key,
    #     Body=io.BytesIO(json_str),
    #     ContentType='application/json'
    # )
    # print(f'algorithm_result.json subido correctamente a MinIO.')



    # #Subimos EL PADRE a minIO
    # for otro in otros:
    #     otro_file_name = otro['file_name']
    #     otro_content = otro['content']
    #     directory = folder_structure.get(os.path.dirname(otro_file_name), "")
    #     #if directory is parent

    #     connection = BaseHook.get_connection('minio_conn')
    #     extra = json.loads(connection.extra)
    #     s3_client = boto3.client(
    #         's3',
    #         endpoint_url=extra['endpoint_url'],
    #         aws_access_key_id=extra['aws_access_key_id'],
    #         aws_secret_access_key=extra['aws_secret_access_key'],
    #         config=Config(signature_version='s3v4')
    #     )

    #     bucket_name = 'missions'  
    #     uuid_key= uuid.uuid4()
    #     key = str(uuid_key) +'/' + otro_file_name

    #     # Subir el archivo a MinIO
    #     s3_client.put_object(
    #         Bucket=bucket_name,
    #         Key=key,
    #         Body=io.BytesIO(otro_content),
    #     )
    #     print(f'{otro_file_name} subido correctamente a MinIO.')

    # try:

    #     id_resource_uuid = uuid_key

    #     query = text("""
    #     INSERT INTO missions.mss_inspection_vegetation_parent
    #     (mission_inspection_id, resource_id, geometry)
    #     VALUES (:mission_inspection_id, :id_resource, :geometry)
    #     """)
    #     session.execute(query, {'mission_inspection_id': mission_inspection_id,'id_resource': id_resource_uuid, 'geometry': '1234'})
    #     session.commit()
    #     print(f"Video {key} registrado en la inspección {mission_inspection_id}")
    # except Exception as e:
    #     session.rollback()
    #     print(f"Error al insertar en mss_inspection_vegetation_parent: {str(e)}")

    # finally:
    #     session.close()
    #     print("Conexión a la base de datos cerrada correctamente")

  
    #     #Subimos LOS HIJOS
    # for otro in otros:
    #     otro_file_name = otro['file_name']
    #     otro_content = otro['content']
    #     directory = folder_structure.get(os.path.dirname(otro_file_name), "")
    #     #if directory is parent

    #     connection = BaseHook.get_connection('minio_conn')
    #     extra = json.loads(connection.extra)
    #     s3_client = boto3.client(
    #         's3',
    #         endpoint_url=extra['endpoint_url'],
    #         aws_access_key_id=extra['aws_access_key_id'],
    #         aws_secret_access_key=extra['aws_secret_access_key'],
    #         config=Config(signature_version='s3v4')
    #     )

    #     bucket_name = 'missions'  
    #     uuid_key= uuid.uuid4()
    #     key = str(uuid_key) +'/' + otro_file_name

    #     # Subir el archivo a MinIO
    #     s3_client.put_object(
    #         Bucket=bucket_name,
    #         Key=key,
    #         Body=io.BytesIO(otro_content),
    #     )
    #     print(f'{otro_file_name} subido correctamente a MinIO.')

    # try:

    #     id_resource_uuid = uuid_key

    #     query = text("""
    #     INSERT INTO missions.mss_inspection_vegetation_parent
    #     (mission_inspection_id, resource_id, geometry)
    #     VALUES (:mission_inspection_id, :id_resource, :geometry)
    #     """)
    #     session.execute(query, {'mission_inspection_id': mission_inspection_id,'id_resource': id_resource_uuid, 'geometry': '1234'})
    #     session.commit()
    #     print(f"Video {key} registrado en la inspección {mission_inspection_id}")
    # except Exception as e:
    #     session.rollback()
    #     print(f"Error al insertar en mss_inspection_vegetation_parent: {str(e)}")

    # finally:
    #     session.close()
    #     print("Conexión a la base de datos cerrada correctamente")





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
    'vegetacion',
    default_args=default_args,
    description='Flujo de datos de entrada de elementos de vegetacion',
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
