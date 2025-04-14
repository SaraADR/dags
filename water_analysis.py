import base64
import json
import re
import tempfile
import uuid
import zipfile
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
import io
from sqlalchemy import  text
from dag_utils import upload_to_minio_path, get_geoserver_connection, get_minio_client, execute_query
import os
from airflow.models import Variable
import copy
import pytz
from airflow.hooks.base_hook import BaseHook
import requests
import logging
from airflow.providers.ssh.hooks.ssh import SSHHook

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

        if(archivo_file_name.endswith('.tif')):
            print(type(archivo_content))
            s3_client.put_object(
                Bucket=bucket_name,
                Key=archivo_key,
                Body=io.BytesIO(archivo_content),
                ContentType="image/tiff"
            )
            print(f'{archivo_file_name} subido correctamente a MinIO.')
        else:
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

    #Geoserver
    publish_to_geoserver(archivos)
    #Integramos en geonetwork
    # generar_metadato_xml(json_modificado)
    # create_metadata_uuid_basica(archivos, json_modificado)



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


def publish_to_geoserver(archivos, **context):
    WORKSPACE = "USV_Water_analysis_2025"
    GENERIC_LAYER = "spain_water_analysis"

    if not archivos:
        raise Exception("No hay archivos para subir a GeoServer.")
    
    # Subida a GeoServer
    base_url, auth = get_geoserver_connection("geoserver_connection")
    temp_files = []

    for archivo in archivos:
        archivo_file_name = archivo['file_name']
        archivo_content = base64.b64decode(archivo['content'])
        archivo_extension = os.path.splitext(archivo_file_name)[1]

        # Guardar el archivo en el sistema antes de usarlo
        with tempfile.NamedTemporaryFile(delete=False, suffix=archivo_extension) as temp_file:
            temp_file.write(archivo_content)
            temp_file_path = temp_file.name  
            temp_files.append(temp_file_path)



    tiff_files = [temp_file[0] for temp_file in temp_files if temp_file[1] == ".tif"]

    for tif_file in tiff_files:
        with open(tif_file, 'rb') as f:
            file_data = f.read()
        
        layer_name = f"USV_Water_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        headers = {"Content-type": "image/tiff"}

        # Publicar capa raster en GeoServer
        url_new = f"{base_url}/workspaces/{WORKSPACE}/coveragestores/{layer_name}/file.geotiff"
        response = requests.put(url_new, headers=headers, data=file_data, auth=auth, params={"configure": "all"})
        if response.status_code not in [201, 202]:
            raise Exception(f"Error publicando {layer_name}: {response.text}")
        print(f"Capa raster publicada: {layer_name}")

        # Actualizar capa genérica
        url_latest = f"{base_url}/workspaces/{WORKSPACE}/coveragestores/{GENERIC_LAYER}/file.geotiff"
        response_latest = requests.put(url_latest, headers=headers, data=file_data, auth=auth, params={"configure": "all"})
        if response_latest.status_code not in [201, 202]:
            raise Exception(f"Error actualizando capa genérica: {response_latest.text}")
        print(f"Capa genérica raster actualizada: {GENERIC_LAYER}")


    shp_files = [archivo['file_name'] for archivo in archivos if archivo['file_name'].lower().endswith(('.shp', '.dbf', '.shx', '.prj', '.cpg'))]
    if shp_files:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
            for archivo in archivos:
                if archivo['file_name'].lower().endswith(('.shp', '.dbf', '.shx', '.prj', '.cpg')):
                    archivo_content = base64.b64decode(archivo['content'])
                    zip_file.writestr(os.path.basename(archivo['file_name']), archivo_content)
        zip_buffer.seek(0)

        datastore_name = f"vector_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        url = f"{base_url}/workspaces/{WORKSPACE}/datastores/{datastore_name}/file.shp"
        headers = {"Content-type": "application/zip"}

        response = requests.put(url, headers=headers, data=zip_buffer, auth=auth, params={"configure": "all"})
        if response.status_code not in [201, 202]:
            raise Exception(f"Error subiendo vectorial {datastore_name}: {response.text}")
        print(f"Capa vectorial publicada: {datastore_name}")

    print("✅ Publicación en GeoServer completada exitosamente.")

    # # Capa histórica
    # url_new = f"{base_url}/workspaces/{WORKSPACE}/coveragestores/{layer_name}/file.geotiff"
    # response = requests.put(url_new, headers=headers, data=file_data, auth=auth, params={"configure": "all"})
    # if response.status_code not in [201, 202]:
    #     raise Exception(f"Error publicando {layer_name}: {response.text}")
    # print(f"Capa publicada: {layer_name}")

    # # Capa genérica
    # url_latest = f"{base_url}/workspaces/{WORKSPACE}/coveragestores/{GENERIC_LAYER}/file.geotiff"
    # response_latest = requests.put(url_latest, headers=headers, data=file_data, auth=auth, params={"configure": "all"})
    # if response_latest.status_code not in [201, 202]:
    #     raise Exception(f"Error actualizando capa genérica: {response_latest.text}")
    # print(f"Capa genérica actualizada: {GENERIC_LAYER}")











 

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



def generar_metadato_xml(json_modificado, **kwargs):
    ruta_xml = os.path.join(os.path.dirname(__file__), 'recursos', 'archivo_xml_water.xml')

    with open(ruta_xml, 'r') as f:
        contenido = f.read()



    #Leemos rutas del json
    recursos = json_modificado.get("executionResources", [])
    csv_url = None
    informe_url = None

    for recurso in recursos:
        path = recurso.get("path", "")
        if path.endswith(".csv"):
            csv_url = f"{path}"
        elif path.endswith(".pdf"):
            informe_url = f"{path}"


    # Definir manualmente los valores a reemplazar
    titulo = "Datos procesados USV"
    fecha =  datetime.now()
    fecha_completa = datetime.now()
    descripcion = "Fichero que se genera con la salida del algoritmo de Aguas"
    # min_latitud = "-34.65"
    # max_latitud = "-34.55"
    # min_longitud = "-58.52"
    # max_longitud = "-58.40"
    wms_server_shp = "https://wms.miapp.com/capas/uso_suelo"


    # Reemplazar cada variable del XML
    contenido = contenido.replace("${TITULO}", titulo)
    contenido = contenido.replace("${FECHA}", fecha)
    contenido = contenido.replace("${FECHA_COMPLETA}", fecha_completa)
    contenido = contenido.replace("${DESCRIPCION}", descripcion)
    contenido = contenido.replace("${MIN_LATITUD}", min_latitud)
    contenido = contenido.replace("${MAX_LATITUD}", max_latitud)
    contenido = contenido.replace("${MIN_LONGITUD}", min_longitud)
    contenido = contenido.replace("${MAX_LONGITUD}", max_longitud)
    contenido = contenido.replace("${WMS_SERVER_SHP}", wms_server_shp)
    contenido = contenido.replace("${CSV_URL}", csv_url)
    contenido = contenido.replace("${INFORME_URL}", informe_url)

    # Guardar el XML ya con los valores insertados (opcional)
    ruta_final = os.path.join(os.path.dirname(__file__), 'recursos', 'metadato_generado.xml')
    with open(ruta_final, 'w') as f:
        f.write(contenido)

    return contenido



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

    connection = BaseHook.get_connection("geonetwork_update_conn")
    upload_url = f"{connection.schema}{connection.host}/geonetwork/srv/api/records"
    access_token, xsrf_token, set_cookie_header = get_geonetwork_credentials()

    with open('/resursos/algoritmo_xml_water.xml', 'r') as f:
            metadata_template = f.read()

    ruta_xml = os.path.join(os.path.dirname(__file__), 'recursos', 'metadato.xml')

    metadata_rendered = metadata_template
    for key, value in json_modificado.items():
        metadata_rendered = metadata_rendered.replace(f"${{{key}}}", str(value))

    metadata_headers = {
        'Authorization': f"Bearer {access_token}",
        'x-xsrf-token': str(xsrf_token),
        'Cookie': str(set_cookie_header[0]),
        'Accept': 'application/json',
        'Content-Type': 'application/xml'
    }

    metadata_response = requests.post(
        upload_url,
        headers=metadata_headers,
        data=metadata_rendered.encode("utf-8")
    )

    logging.info(f"Subida del metadato XML: {metadata_response.status_code}, {metadata_response.text}")
    metadata_response.raise_for_status()
    response_data = metadata_response.json()
    metadata_infos = response_data.get("metadataInfos", {})
    main_uuid = None
    if metadata_infos:
        values = list(metadata_infos.values())[0]
        if values:
            main_uuid = values[0].get("uuid")

    if not main_uuid:
        raise Exception("No se obtuvo UUID del metadato principal.")



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