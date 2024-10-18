import base64
import json
import os
import re
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
    id_mission = get_idmission(json_content)
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
                bucket_name = 'missions'  
          
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
    

    #Sacamos el BBOX y Subimos los datos a las tablas correspondientes
    for resource in json_content['executionResources']:
        for data_item in resource['data']:
            if data_item['name'] == 'BBOX':
                bbox = data_item['value']
                reference_system = data_item['ReferenceSystem']
    if bbox:
        # Construir el POLYGON usando los valores de BBOX
        polygon_wkt = f"SRID={reference_system};POLYGON(({bbox['westBoundLongitude']} {bbox['southBoundLatitude']}, {bbox['eastBoundLongitude']} {bbox['southBoundLatitude']}, {bbox['eastBoundLongitude']} {bbox['nortBoundLatitude']}, {bbox['westBoundLongitude']} {bbox['nortBoundLatitude']}, {bbox['westBoundLongitude']} {bbox['southBoundLatitude']}))"
        print(polygon_wkt)

        #Guardamos el padre en mss_inspection_vegetation_parent
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
            result = session.execute(query, {'id_resource': parent, 'minspectionId': mission_inspection_id, 'geometry': polygon_wkt})
            inserted_id = result.fetchone()[0]
            session.commit()
            print(f"Registro insertado correctamente con ID: {inserted_id}")

            print(f"mss_inspection_vegetation_parent subido correctamente")
        except Exception as e:
            session.rollback()
            print(f"Error al insertar video en mss_inspection_vegetation_parent: {str(e)}")
    else:
        print("No se encontró BBOX en el JSON.")


    #Guardar los hijos dentro de mss_inspection_vegetation_child
    try:
        index = 0
        for folder, unique_id in childanduuid:
            print(f"Procesando carpeta: {folder}")
            print(f"ID del Child: {unique_id}")

            bbox = get_bbox_for_child(json_content, folder)
            reference = get_referenceSystem_for_path(json_content,folder)
            
            if bbox:
                west = bbox['westBoundLongitude']
                east = bbox['eastBoundLongitude']
                south = bbox['southBoundLatitude']
                north = bbox['nortBoundLatitude']

                # Crear la geometría POLYGON
                polygon = f"SRID={reference};POLYGON(({west} {north}, {east} {north}, {east} {south}, {west} {south}, {west} {north}))"
                print(f"Generado POLYGON: {polygon}")

            query = text("""
            INSERT INTO missions.mss_inspection_vegetation_child
            ( vegetation_parent_id, resource_id, review_status_id, geometry)
            VALUES( :parentId, :id_resource, :reviewStatus, :geometry) RETURNING id;;
            """)      
            result = session.execute(query, {'parentId': inserted_id, 'id_resource': unique_id, 'reviewStatus': 2,'geometry': polygon})
            new_inserted_id  = result.fetchone()[0]
            session.commit()
            print(f"mss_inspection_vegetation_child subido correctamente con ID: {new_inserted_id}")
            childanduuid[index].append(new_inserted_id)
            index = index +1


    except Exception as e:
        session.rollback()
        print(f"Error al insertar video en mss_inspection_vegetation_child: {str(e)}")
    finally:
        session.close()
        print("Conexión a la base de datos cerrada correctamente")



    #Guardamos los conflictos de cada hijo
    try:
        for folder, unique_id, inserted_id in childanduuid:
            print(f"Procesando carpeta: {folder}")
            print(f"ID del Child: {unique_id}")

            conflicts = get_conflicts_for_path(json_content, folder)
            print(conflicts)

            
            print("SACANDO POLYGON")
            if conflicts:
                for conflict in conflicts:
                    conflict_data = {}
                    center_coords = None
                    for conflict_item in conflict['data']:
                        conflict_data[conflict_item['name']] = conflict_item['value']

                        # Extraer los datos que te interesan
                        alert_level = conflict_data.get('AlertLevel', 0)
                        detected_obj_type = conflict_data.get('DetectedObjType', None)
                        impact = conflict_data.get('Impact', None)
                        line_height = conflict_data.get('LineHeight', None)
                        distance = conflict_data.get('Distance', None)

                        if conflict_item['name'] == 'CenterCords':
                            center_coords = conflict_item['value']
                            reference_system = conflict_item.get('ReferenceSystem', None)

                        if center_coords and 'lat' in center_coords and 'lon' in center_coords:
                            lat = center_coords['lat']
                            lon = center_coords['lon']
                            point_geometry = f"SRID={reference_system};POINT({lon} {lat})"
                        else:
                            point_geometry = None

                    query = text("""
                    INSERT INTO missions.mss_inspection_vegetation_conflict
                    ( vegetation_child_id, resource_id, impact, obj_detected_type, line_height, alertlevel, distance, geometry, review_status_id)
                    VALUES( :vegchild_id, NULL, :impact, :detected_type, :line_height, :alert_level, :distance, :geometry, :rev_status);
                    """)      
                    session.execute(query, {'vegchild_id': inserted_id, 'impact': impact, 'detected_type': detected_obj_type,'line_height': line_height, 'alert_level': alert_level, 'distance': distance, 'geometry': point_geometry, 'rev_status': 1})
                    session.commit()
                    print(f"mss_inspection_vegetation_conflict subido correctamente")
                    
    except Exception as e:
        session.rollback()
        print(f"Error al insertar video en mss_inspection_vegetation_conflicts: {str(e)}")
    finally:
        session.close()
        print("Conexión a la base de datos cerrada correctamente")




def get_idmission(data):
    for metadata in data['metadata']:
        if metadata['name'] == 'MissionID':
            return metadata['value']
    return None


def get_bbox_for_child(data, path_contains):
    # Iterar sobre los recursos en executionResources
    for resource in data['executionResources']:
        for item in resource['data']:
            if item['name'] == 'children':
                for child in item['value']:
                    if path_contains in child['path']:
                        for child_item in child['data']:
                            if child_item['name'] == 'BBOX':
                                return child_item['value']
    return None

def get_referenceSystem_for_path(data, path_contains):
    for resource in data['executionResources']:
        for item in resource['data']:
            if item['name'] == 'children':
                for child in item['value']:
                    if path_contains in child['path']:
                        for child_item in child['data']:
                            if child_item['name'] == 'BBOX':
                                return child_item.get('ReferenceSystem', None)
    return None

def get_conflicts_for_path(data, path_contains):
    for resource in data['executionResources']:
        if path_contains in resource['path'] and 'CloudCut' in resource['tag']:
            for item in resource['data']:
                if item['name'] == 'conflicts':
                    return item['value']
    return []




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

#Generar notificación de vuelta
generate_notify = PythonOperator(
    task_id='generate_notify_job',
    python_callable=generate_notify_job,
    provide_context=True,
    dag=dag,
)


process_extracted_files_task >> generate_notify
