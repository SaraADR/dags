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
    # Obtenemos los archivos 
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



    # Agrupamos 'otros' por carpetas utilizando regex para extraer el prefijo de la carpeta
    grouped_files = defaultdict(list)
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


    #Subimos a minIO cada carpeta con sus archivos correspondientes
    childanduuid = []
    parent = None
    for folder, files in grouped_files.items():
            try:
                
                unique_id_child = str(uuid.uuid4())
                if "cloudCut" in folder:
                      childanduuid.append([folder, unique_id_child])
                else:
                      parent = unique_id_child

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
                    print(f'{os.path.basename(file['file_name'])} subido correctamente a MinIO.')

            except Exception as e:
                print(f"Error al insertar en minio: {str(e)}")


    #Subimos el algorithm_result a la carpeta del padre
    try:
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

        bucket_name = 'avincis-test'  
        json_key = parent +'/' + 'algorithm_result.json'

        # Subir el archivo a MinIO
        s3_client.put_object(
            Bucket=bucket_name,
            Key=json_key,
            Body=io.BytesIO(content_bytes),
        )
        print(f'algorithm_result subido correctamente a MinIO.')
    except Exception as e:
                print(f"Error al insertar en minio: {str(e)}")


    print(childanduuid)
    print(parent)

    #Buscamos el mission_inspection correspondiente a estos archivos
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
            return None

    except Exception as e:
        session.rollback()
        print(f"Error durante la busqueda del mission_inspection: {str(e)}")
        
    print(f"row: {row}")
    

    #Subimos los datos a las tablas correspondientes

    try:

        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()
    
        query = text("""
            INSERT INTO missions.mss_inspection_vegetation_parent
            (mission_inspection_id, resource_id, geometry)
            VALUES(:minspectionId, :id_resource, :geometry)
            RETURNING id;  
            """)
        print(f'minspectionId {mission_inspection_id} + idRecurso {parent}')
        result = session.execute(query, {'id_resource': parent, 'minspectionId': mission_inspection_id, 'geometry': 'SRID=25829;POLYGON ((603067.82 4739032.36, 603634.54 4739032.36, 603634.54 4737037.27, 603067.82 4737037.27, 603067.82 4739032.36))'})
        inserted_id = result.fetchone()[0]
        session.commit()
        print(f"Registro insertado correctamente con ID: {inserted_id}")

        print(f"mss_inspection_vegetation_parent subido correctamente")
    except Exception as e:
        session.rollback()
        print(f"Error al insertar video en mss_inspection_vegetation_parent: {str(e)}")


    try:
        for folder, unique_id in childanduuid:
            print(f"Procesando carpeta: {folder}")
            print(f"ID del Child: {unique_id}")

            query = text("""
            INSERT INTO missions.mss_inspection_vegetation_child
            ( vegetation_parent_id, resource_id, review_status_id, geometry)
            VALUES( :parentId, :id_resource, :reviewStatus, :geometry);
            """)      
            session.execute(query, {'parentId': inserted_id, 'id_resource': unique_id, 'reviewStatus': 2,'geometry': 'SRID=25829;POLYGON ((603067.82 4739032.36, 603634.54 4739032.36, 603634.54 4737037.27, 603067.82 4737037.27, 603067.82 4739032.36))'})
            session.commit()
            print(f"mss_inspection_vegetation_child subido correctamente")
    except Exception as e:
        session.rollback()
        print(f"Error al insertar video en mss_inspection_vegetation_child: {str(e)}")




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
