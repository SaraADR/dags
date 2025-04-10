import base64
import json
import re
import uuid
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
import io
from sqlalchemy import  text
from dag_utils import get_db_session, get_minio_client, execute_query
import os
from airflow.models import Variable
import copy
import pytz
from airflow.hooks.base_hook import BaseHook
import requests
import logging

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


    json_modificado = copy.deepcopy(json_content)
    rutaminio = Variable.get("ruta_minIO")
    nuevos_paths = {}
    for archivo in archivos:
        archivo_file_name = os.path.basename(archivo['file_name'])
        archivo_content = base64.b64decode(archivo['content'])

        s3_client = get_minio_client()


        bucket_name = 'missions'
        archivo_key = f"{id_mission}/{uuid_key}/{archivo_file_name}"


        s3_client.put_object(
            Bucket=bucket_name,
            Key=archivo_key,
            Body=io.BytesIO(archivo_content),
        )
        print(f'{archivo_file_name} subido correctamente a MinIO.')

        print(archivo_key)
        nuevos_paths[archivo_file_name] = f"{rutaminio}/{bucket_name}/{archivo_key}"
      
    for resource in json_modificado['executionResources']:
        file_name = os.path.basename(resource['path'])
        if file_name in nuevos_paths:
            print(f"Actualizando path de {file_name}")
            resource['path'] = nuevos_paths[file_name]



    startTimeStamp = json_modificado['startTimestamp']
    endTimeStamp = json_modificado['endTimestamp']

    json_str = json.dumps(json_modificado).encode('utf-8')
    json_key = f"{id_mission}/{uuid_key}/algorithm_result.json"

    s3_client.put_object(
        Bucket='missions',
        Key=json_key,
        Body=io.BytesIO(json_str),
        ContentType='application/json'
    )
    print(f'Archivo JSON subido correctamente a MinIO.')

    #Historizamos para guardar en bd y pantalla de front
    historizacion(json_modificado, json_content, id_mission, startTimeStamp, endTimeStamp )

    #Integramos en geonetwork
    create_metadata_uuid_basica(archivos, json_modificado)



def historizacion(output_data, input_data, mission_id, startTimeStamp, endTimeStamp):
    try:
        # Y guardamos en la tabla de historico
            print("Guardamos en historización")
            madrid_tz = pytz.timezone('Europe/Madrid')

            # Formatear phenomenon_time
            start_dt = datetime.strptime(startTimeStamp, "%Y%m%dT%H%M%S")
            end_dt = datetime.strptime(endTimeStamp, "%Y%m%dT%H%M%S")

            phenomenon_time = f"[{start_dt.strftime('%Y-%m-%dT%H:%M:%S')}, {end_dt.strftime('%Y-%m-%dT%H:%M:%S')}]"
            datos = {
                'sampled_feature': mission_id, 
                'result_time': datetime.now(madrid_tz),
                'phenomenon_time': phenomenon_time,
                'input_data': json.dumps(input_data),
                'output_data': json.dumps(output_data)
            }


            # Construir la consulta de inserción
            query = f"""
                INSERT INTO algoritmos.algoritmo_water_analysis (
                    sampled_feature, result_time, phenomenon_time, input_data, output_data
                ) VALUES (
                    {datos['sampled_feature']},
                    '{datos['result_time']}',
                    '{datos['phenomenon_time']}'::TSRANGE,
                    '{datos['input_data']}',
                    '{datos['output_data']}'
                )
            """

            # Ejecutar la consulta
            execute_query('biobd', query)
    except Exception as e:
        print(f"Error en el proceso: {str(e)}")    
 

def get_geonetwork_credentials():
    try:

        conn = BaseHook.get_connection('geonetwork_conn')
        credential_dody = {
            "username" : conn.login,
            "password" : conn.password
        }

        # Hacer la solicitud para obtener las credenciales
        logging.info(f"Obteniendo credenciales de: {conn.host}")
        response = requests.post(conn.host,json= credential_dody)

        # Verificar que la respuesta sea exitosa
        response.raise_for_status()

        # Extraer los headers y tokens necesarios
        response_object = response.json()
        access_token = response_object['accessToken']
        xsrf_token = response_object['xsrfToken']
        set_cookie_header = response_object['setCookieHeader']
    

        return [access_token, xsrf_token, set_cookie_header]
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al obtener credenciales: {e}")
        raise Exception(f"Error al obtener credenciales: {e}")


def create_metadata_uuid_basica(archivos, json_modificado):
    # Se crear un metadato compatible con GeoNetwork
    mission_id = json_modificado["metadata"][1]["value"]
    date = json_modificado["metadata"][6]["value"]
    operator = json_modificado["metadata"][4]["value"]
    aircraft = json_modificado["metadata"][2]["value"]
    tags = list({tag for r in json_modificado["executionResources"] for tag in r["tag"].split(",")})

    generated_metadata = {
        "title": f"Análisis de Agua - Misión {mission_id}",
        "abstract": f"Resultado del análisis realizado con aeronave {aircraft} el {date}. Operador: {operator}.",
        "date": date,
        "keywords": tags,
        "contact": {
            "name": operator,
            "email": "contacto@ejemplo.com"
        },
        "language": "spa",
        "format": "application/json"
    }

    metadata_file = io.BytesIO()
    metadata_file.write(json.dumps(generated_metadata).encode('utf-8'))
    metadata_file.seek(0)


    connection = BaseHook.get_connection("geonetwork_update_conn")
    access_token, xsrf_token, set_cookie_header = get_geonetwork_credentials()
    headers = {
    'Authorization': f"Bearer {access_token}",
    'x-xsrf-token': str(xsrf_token),
    'Cookie': str(set_cookie_header[0]),
    'Accept': 'application/json'
    }

    files = {
        'file': ('metadata.json', metadata_file, 'application/json')
    }
    # 3. Subir el metadato
    response = requests.post(
        f"{connection.schema}{connection.host}/geonetwork/srv/api/records",
        headers=headers,
        files=files
    )
    
    print("Respuesta de creación de metadato:", response.status_code, response.text)
    response.raise_for_status()

    uuid_metadata = list(response.json()['metadataInfos'].values())[0][0]['uuid']
    print("📄 UUID del metadato creado:", uuid_metadata)
    return uuid_metadata


def upload_to_geonetwork(archivos, json_modificado, **context):
    try:
        connection = BaseHook.get_connection("geonetwork_update_conn")
        upload_url = f"{connection.schema}{connection.host}/geonetwork/srv/api/records"
        access_token, xsrf_token, set_cookie_header = get_geonetwork_credentials()

        # UUID común para agrupar todos los recursos
        group_uuid = str(uuid.uuid4())
        logging.info(f"UUID común para agrupación en GeoNetwork: {group_uuid}")


        resource_ids = []

        for archivo in archivos:
            archivo_name = archivo['file_name']
            archivo_content = base64.b64decode(archivo['content'])

            if archivo_name.lower().endswith('.png'):
                mime_type = 'image/png'
                logging.info(f"Procesando archivo PNG: {archivo_name}")
            else:
                mime_type = 'application/octet-stream'

            files = {
                'file': (archivo_name, archivo_content, mime_type),
            }

            logging.info(f"XML DATA: {archivo_name}")


            headers = {
                'Authorization': f"Bearer {access_token}",
                'x-xsrf-token': str(xsrf_token),
                'Cookie': str(set_cookie_header[0]),
                'Accept': 'application/json'
            }

            response = requests.post(upload_url, files=files, headers=headers)
            logging.info(f"Respuesta completa de GeoNetwork: {response.status_code}, {response.text}")

            response.raise_for_status()
            response_data = response.json()

            # Extraer el identificador correcto desde metadataInfos
            metadata_infos = response_data.get("metadataInfos", {})
            if metadata_infos:
                metadata_values = list(metadata_infos.values())[0]  # Obtener la primera lista de metadatos
                if metadata_values:
                    resource_id = metadata_values[0].get("uuid")  # Extraer el verdadero identificador
                else:
                    resource_id = None
            else:
                resource_id = None

            if not resource_id:
                logging.error(f"No se encontró un identificador válido en la respuesta de GeoNetwork: {response_data}")
                continue

            logging.info(f"Identificador del recurso en GeoNetwork: {resource_id}")
            resource_ids.append(resource_id)

        if not resource_ids:
            raise Exception("No se generó ningún resource_id en GeoNetwork.")


        context['ti'].xcom_push(key='resource_id', value=resource_ids)
        context['ti'].xcom_push(key='group_uuid', value=group_uuid)
        return resource_ids


    except Exception as e:
        logging.error(f"Error al subir el archivo a GeoNetwork: {e}")
        raise


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
    'water_analysis',
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

# generate_notify = PythonOperator(
#     task_id='generate_notify_job',
#     python_callable=generate_notify_job,
#     provide_context=True,
#     dag=dag,
# )

# Flujo del DAG
process_extracted_files_task 