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
import geopandas as gpd


# Función para procesar archivos extraídos
def process_extracted_files(**kwargs):
    archivos = kwargs['dag_run'].conf.get('otros', [])
    json_content = kwargs['dag_run'].conf.get('json')

    if not json_content:
        print("Ha habido un error con el traspaso de los documentos")
        return
    print("Archivos para procesar preparados")


    #Extraemos el mission ID
    id_mission = None
    for metadata in json_content['metadata']:
        if metadata['name'] == 'MissionID':
            id_mission = metadata['value']
            break

    print(f"MissionID: {id_mission}")


    #Preparamos la subida a minIO de todos los archivos
    json_modificado = copy.deepcopy(json_content)
    rutaminio = Variable.get("ruta_minIO")
    nuevos_paths = {}
    uuid_key = uuid.uuid4()
    print(f"UUID generado para almacenamiento {uuid_key}")  

    s3_client = get_minio_client()
    bucket_name = 'missions'


    #Subimos el zip de seaFloor a minIO
    archivos_path, zip_path = hacerZipConSeaFloor(json_content, archivos)
    
    zip_file_name = os.path.basename(zip_path)
    zip_key = f"{id_mission}/{uuid_key}/{zip_file_name}"
    with open(zip_path, 'rb') as zip_file:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=zip_key,
            Body=zip_file,
            ContentType="application/zip"
        )
    print(f'Archivo ZIP {zip_file_name} subido correctamente a MinIO.')


    #Subimos los archivos todos por separado
    for archivo in archivos:
        archivo_file_name = os.path.basename(archivo['file_name'])
        archivo_content = base64.b64decode(archivo['content'])

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
      


    #Prepoaramos y ejecutamos la historización
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

    #Metodo para la historización
    historizacion(json_modificado, json_content, id_mission, startTimeStamp, endTimeStamp )


    #Subimos a Geoserver el tif y ambos shapes
    layer_name, workspace, base_url = publish_to_geoserver(archivos)


    #integramos con geonetwork
    xml_data = generate_dynamic_xml(json_content)
    resources_id = upload_to_geonetwork_xml([xml_data])
    upload_tiff_attachment(resources_id, xml_data, archivos)
    link_geoserver_wms_to_metadata(resources_id, layer_name, workspace, base_url)




#ZIP DE DATOS SEAFLOOR
def hacerZipConSeaFloor(json, archivos):
    archivos_zip_paths = []

    tmp_dir = "/tmp/seafloor_zip"
    os.makedirs(tmp_dir, exist_ok=True)

    for recurso in json['executionResources']:
        tags = recurso.get('tag', '')
        if 'SeaFloor' in tags:
            path = os.path.basename(recurso.get('path', ''))
            
            # Buscar en archivos cargados
            for archivo in archivos:
                if os.path.basename(archivo['file_name']) == path:
                    content = base64.b64decode(archivo['content'])
                    output_file_path = os.path.join(tmp_dir, path)
                    
                    with open(output_file_path, 'wb') as f:
                        f.write(content)
                    
                    archivos_zip_paths.append(output_file_path)
                    break
    output_zip_path = os.path.join(tmp_dir, 'seafloor.zip')
    with zipfile.ZipFile(output_zip_path, 'w') as zipf:
        for file_path in archivos_zip_paths:
            zipf.write(file_path, arcname=os.path.basename(file_path))
    print(f"ZIP generado con archivos SeaFloor en: {output_zip_path}")
    return archivos_zip_paths, output_zip_path



#HISTORIZACION
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



#PUBLICAR GEO
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
            temp_files.append((archivo_file_name, temp_file_path))



    #SUBIMOS LOS TIFFS
    tiff_files = [path for name, path in temp_files if name.lower().endswith(".tif")]

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
        print(f"Raster disponible en: {base_url}/geoserver/{WORKSPACE}/wms?layers={WORKSPACE}:{layer_name}")
    
    

    #SUBIMOS LOS SHAPES
    water_analysis_files = []
    seafloor_files = []
    for original_name, temp_path in temp_files:
        if os.path.splitext(original_name)[1].lower() in ('.shp', '.dbf', '.shx', '.prj', '.cpg'):
            if "wateranalysis" in original_name.lower():
                water_analysis_files.append((original_name, temp_path))
            elif "seafloor" in original_name.lower():
                seafloor_files.append((original_name, temp_path))

    subir_zip_shapefile(water_analysis_files, "waterAnalysis", WORKSPACE, base_url, auth)
    subir_zip_shapefile(seafloor_files, "seaFloor", WORKSPACE, base_url, auth)
    #set_geoserver_style("SeaFloor", base_url, auth, "SeaFloorStyle")

    print("----Publicación en GeoServer completada exitosamente.----")
    return layer_name, WORKSPACE, base_url




def subir_zip_shapefile(file_group, nombre_capa, WORKSPACE, base_url, auth):
        if not file_group:
            return

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
            for nombre_original, ruta_temporal in file_group:
                with open(ruta_temporal, 'rb') as f:
                    zip_file.writestr(nombre_original, f.read())
        zip_buffer.seek(0)

        datastore_name = f"{nombre_capa}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        url = f"{base_url}/workspaces/{WORKSPACE}/datastores/{datastore_name}/file.shp"
        headers = {"Content-type": "application/zip"}

        response = requests.put(url, headers=headers, data=zip_buffer, auth=auth, params={"configure": "all"})
        if response.status_code not in [201, 202]:
            raise Exception(f"Error subiendo vectorial {datastore_name}: {response.text}")
        print(f"Capa vectorial publicada: {datastore_name}")
        print(f"🗺️  Vector disponible en: {base_url}/geoserver/{WORKSPACE}/wms?layers={WORKSPACE}:{datastore_name}")


def set_geoserver_style(layer_name, base_url, auth, style_name, workspace="USV_Water_analysis_2025"):
    url = f"{base_url}/layers/{workspace}:{layer_name}"
    headers = {"Content-Type": "application/xml"}
    payload = f"""
        <layer>
            <defaultStyle>
                <name>{style_name}</name>
            </defaultStyle>
        </layer>
    """
    response = requests.put(url, headers=headers, data=payload, auth=auth)
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Error asignando estilo a {layer_name}: {response.text}")
    
    print(f"✅ Estilo '{style_name}' aplicado correctamente a la capa '{layer_name}'")


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




def generate_dynamic_xml(json_modificado):

    file_identifier = ""
    title = ""

    for metadata in json_modificado['metadata']:
        if metadata['name'] == 'ExecutionID':
            file_identifier = metadata['value']
            break
        if metadata['name'] == 'AlgorithmID':
            title = metadata['value']
            break

    date = json_modificado['endTimestamp']

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
    <gmd:MD_Metadata xmlns:gmd="http://www.isotc211.org/2005/gmd"
                    xmlns:gco="http://www.isotc211.org/2005/gco"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xsi:schemaLocation="http://www.isotc211.org/2005/gmd
                    http://schemas.opengis.net/iso/19139/20070417/gmd/gmd.xsd">
    <gmd:fileIdentifier>
        <gco:CharacterString>{file_identifier}</gco:CharacterString>
    </gmd:fileIdentifier>
    <gmd:dateStamp>
        <gco:Date>{date}</gco:Date>
    </gmd:dateStamp>
    <gmd:identificationInfo>
        <gmd:MD_DataIdentification>
        <gmd:citation>
            <gmd:CI_Citation>
            <gmd:title>
                <gco:CharacterString>{title}</gco:CharacterString>
            </gmd:title>
            </gmd:CI_Citation>
        </gmd:citation>
        </gmd:MD_DataIdentification>
    </gmd:identificationInfo>
    </gmd:MD_Metadata>
    """
    print(xml)
    xml_encoded = base64.b64encode(xml.encode('utf-8')).decode('utf-8')
    return xml_encoded




def upload_to_geonetwork_xml(xml_data_array):
        try:
            connection = BaseHook.get_connection("geonetwork_update_conn")
            upload_url = f"{connection.schema}{connection.host}/geonetwork/srv/api/records"
            access_token, xsrf_token, set_cookie_header = get_geonetwork_credentials()

            resource_ids = []

            for xml_data in xml_data_array:
                xml_decoded = base64.b64decode(xml_data).decode('utf-8')

                files = {
                    'file': ('metadata.xml', xml_decoded, 'text/xml'),
                }

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

                metadata_infos = response_data.get("metadataInfos", {})
                if metadata_infos:
                    metadata_values = list(metadata_infos.values())[0]
                    if metadata_values:
                        resource_id = metadata_values[0].get("uuid")
                    else:
                        resource_id = None
                else:
                    resource_id = None

                if not resource_id:
                    logging.error(f"No se encontró un identificador válido en la respuesta: {response_data}")
                    continue

                logging.info(f"UUID del recurso: {resource_id}")
                resource_ids.append(resource_id)

            if not resource_ids:
                raise Exception("No se generó ningún resource_id.")

            return resource_ids

        except Exception as e:
            logging.error(f"Error al subir el archivo a GeoNetwork: {e}")
            raise


def upload_tiff_attachment(resource_ids, metadata_input, archivos):
        connection = BaseHook.get_connection("geonetwork_update_conn")
        base_url = f"{connection.schema}{connection.host}/geonetwork/srv/api"
        access_token, xsrf_token, set_cookie_header = get_geonetwork_credentials()
        geonetwork_url = connection.host 

        for archivo in archivos:
            archivo_file_name = os.path.basename(archivo['file_name'])
            archivo_content = base64.b64decode(archivo['content'])

            ext = os.path.splitext(archivo_file_name)[1].lower()
            mime_type = {
                    '.tif': 'image/tiff',
                    '.tiff': 'image/tiff',
                    '.png': 'image/png',
                    '.pdf': 'application/pdf',
                    '.txt': 'text/plain',
                    '.sh': 'application/x-sh'  
            }.get(ext, 'application/octet-stream')
            
            for uuid in resource_ids:
                files = {
                    'file': (archivo_file_name, io.BytesIO(archivo_content), mime_type),
                }

                headers = {
                    'Authorization': f"Bearer {access_token}",
                    'x-xsrf-token': str(xsrf_token),
                    'Cookie': str(set_cookie_header[0]),
                    'Accept': 'application/json'
                }

                url = f"{geonetwork_url}/geonetwork/srv/api/records/{uuid}/attachments"
                response = requests.post(url, files=files, headers=headers)

                if response.status_code not in [200, 201]:
                    logging.error(f"Error subiendo recurso a {uuid}: {response.status_code} {response.text}")
                    raise Exception("Fallo al subir el adjunto")
                else:
                    logging.info(f"Recurso subido correctamente para {uuid}")


def link_geoserver_wms_to_metadata(resource_ids, layer_name, workspace, geoserver_url):
    connection = BaseHook.get_connection("geonetwork_update_conn")
    access_token, xsrf_token, set_cookie_header = get_geonetwork_credentials()

    wms_url = f"{geoserver_url}/geoserver/{workspace}/wms"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-xsrf-token": xsrf_token,
        "Cookie": set_cookie_header[0],
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    link_fragment = {
        "protocol": "OGC:WMS",
        "name": f"{workspace}:{layer_name}",
        "description": "Servicio WMS generado dinámicamente",
        "url": wms_url
    }

    for uuid in resource_ids:
        response = requests.post(
            f"{connection.schema}{connection.host}/geonetwork/srv/api/records/{uuid}/links",
            json=link_fragment,
            headers=headers
        )

        if response.status_code not in [200, 201]:
            logging.error(f"Error al vincular WMS al metadato {uuid}: {response.status_code} {response.text}")
            raise Exception("Fallo al vincular WMS")
        else:
            logging.info(f"WMS vinculado correctamente a {uuid}")


















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


# Flujo del DAG
process_extracted_files_task 