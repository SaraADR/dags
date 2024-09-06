import base64
import json
import os
import re
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
from collections import defaultdict


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


    grouped_files = defaultdict(list)
    
    # Agrupamos 'otros' por carpetas utilizando regex para extraer el prefijo de la carpeta
    for file_info in otros:
        file_name = file_info['file_name']
        
        match = re.match(r'resources/(cloud[^/]+)/', file_name)
        if match:
            folder_name = match.group(1) 
            grouped_files[folder_name].append(file_info)

    # Mostramos los archivos agrupados por carpetas
    for folder, files in grouped_files.items():
        print(f"Carpeta: {folder}")
        for file_info in files:
            print(f"  Archivo: {file_info['file_name']}")


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

    # procesamos los archivos según la carpeta

    for folder, files in grouped_files.items():
        if "cloudCut" in folder:
            #Subimos los hijos
            print(f"Procesando archivos de {folder} para análisis de corte...")

            try:
                
                unique_id_child = str(uuid.uuid4())
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
          
                for file in files:
                    content_bytes = base64.b64decode(file['content'])
                    actual_child_key = str(unique_id_child) +'/' +  os.path.basename(file['file_name']) 
                    # Subir el archivo a MinIO
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=actual_child_key,
                        Body=io.BytesIO(content_bytes),
                    )
                    print(f'{file_name} subido correctamente a MinIO.')

            except Exception as e:
                print(f"Error al insertar en minio: {str(e)}")


        else:
            #Subimos el padre
            print(f"Procesando archivos de {folder} para análisis general...")
            
            try:
                
                unique_id = str(uuid.uuid4())
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
         
                for file in files:
                    content_bytes = base64.b64decode(file['content'])
                    actual_key = str(unique_id) +'/' + os.path.basename(file['file_name']) 
                    # Subir el archivo a MinIO
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=actual_key,
                        Body=io.BytesIO(content_bytes),
                    )
                    print(f'{file_name} subido correctamente a MinIO.')

            except Exception as e:
                print(f"Error al insertar en minio: {str(e)}")


            print(unique_id_child)
            print(unique_id)

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
