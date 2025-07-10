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
import rasterio
from xml.sax.saxutils import escape
from pyproj import Transformer
from botocore.exceptions import ClientError
from utils.callback_utils import task_failure_callback
import shutil
from water_analysis.utils.xml_generator import generate_dynamic_xml
from water_analysis.utils.geoserver_publicator import publish_to_geoserver
from water_analysis.utils.minio_utils import download_from_minio
from water_analysis.utils.geonetwork_publicator import upload_to_geonetwork_xml, upload_tiff_attachment

# Función para procesar archivos extraídos
def process_extracted_files(**kwargs):
    otros = kwargs['dag_run'].conf.get('otros', [])
    json_content = kwargs['dag_run'].conf.get('json')

    trace_id = kwargs['dag_run'].conf['trace_id']
    print(f"Processing with trace_id: {trace_id}")

    if not json_content:
        print("Ha habido un error con el traspaso de los documentos")
        return
    print("Archivos para procesar preparados")

    s3_client = get_minio_client()
    archivos = []
    file_path_in_minio =  otros.replace('tmp/','')
    folder_prefix = 'sftp/'
    try:
        local_zip_path = download_from_minio(s3_client, 'tmp', file_path_in_minio, 'tmp', folder_prefix)
        print(local_zip_path)  
    except Exception as e:
        print(f"Error al descargar desde MinIO: {e}")
        raise 

    if local_zip_path is None:
        print(f"No se pudo descargar el archivo desde MinIO: {local_zip_path}")
        return
    
    try:
            if not os.path.exists(local_zip_path):
                print(f"Archivo no encontrado: {local_zip_path}")
                return
            

            # Abre y procesa el archivo ZIP desde el sistema de archivos
            with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
                zip_file.testzip() 
                print("El archivo ZIP es válido.")
    except zipfile.BadZipFile:
            print("El archivo no es un ZIP válido antes del procesamiento.")
            return
    
    try:
        with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
            with tempfile.TemporaryDirectory() as temp_dir:
                print(f"Directorio temporal creado: {temp_dir}")
                zip_file.extractall(temp_dir)
                file_list = zip_file.namelist()
                print("Archivos en el ZIP:", file_list)
                archivos = []
                folder_structure = {}
                for file_name in file_list:
                    file_path = os.path.join(temp_dir, file_name)

                    if os.path.isdir(file_path):
                        # Si es un directorio, saltamos
                        continue

                    print(f"Procesando archivo: {file_name}")
                    with open(file_path, 'rb') as f:
                        content = f.read()

                    directory = os.path.dirname(file_name)
                    if directory not in folder_structure:
                        folder_structure[directory] = []
                    folder_structure[directory].append(file_name)
                    if os.path.basename(file_name).lower() == 'algorithm_result.json':
                        continue
                    else:
                        if file_name.lower().endswith('.las') and "cloud-" in file_name:
                                empty_content = b''
                                encoded_empty_content = base64.b64encode(empty_content).decode('utf-8')
                                archivos.append({'file_name': file_name, 'content': encoded_empty_content})
                                print(f"Archivo .las padre {file_name} procesado con 0 bytes")              
                        else:
                            encoded_content = base64.b64encode(content).decode('utf-8')
                            archivos.append({'file_name': file_name, 'content': encoded_content})
                print("Estructura de carpetas y archivos en el ZIP:", folder_structure)

    except zipfile.BadZipFile as e:
        print(f"El archivo no es un ZIP válido: {e}")
        return
    


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
    zip_file_save = f"{rutaminio}/{bucket_name}/{zip_key}"

    ruta_png = None
    ruta_tiff = None
    ruta_csv = None
    ruta_pdf = None
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

            #Sacamos su lot lang
            coordenadas_tif = obtener_coordenadas_tif(archivo_file_name, archivo_content)
            ruta_tiff = f"{rutaminio}/{bucket_name}/{archivo_key}"

        else:
            content_type = "application/octet-stream"  # valor por defecto
            if archivo_file_name.endswith('.png'):
                content_type = "image/png"
                ruta_png = f"{rutaminio}/{bucket_name}/{archivo_key}"
            elif archivo_file_name.endswith('.jpg') or archivo_file_name.endswith('.jpeg'):
                content_type = "image/jpeg"
            elif archivo_file_name.endswith('.csv') :
                content_type = "text/csv"
                ruta_csv = f"{rutaminio}/{bucket_name}/{archivo_key}"
            elif archivo_file_name.endswith('.pdf'):
                content_type = "application/pdf"
                ruta_pdf = f"{rutaminio}/{bucket_name}/{archivo_key}"

            s3_client.put_object(
                Bucket=bucket_name,
                Key=archivo_key,
                Body=io.BytesIO(archivo_content),
                ContentType=content_type
            )
            print(f'{archivo_file_name} subido correctamente a MinIO.')

            print(f"archivo_key {archivo_key}"  )
        nuevos_paths[archivo_file_name] = f"{rutaminio}/{bucket_name}/{archivo_key}"
    print(f"Rutapng: {ruta_png}")


    #Preparamos y ejecutamos la historización
    for resource in json_modificado['executionResources']:
        file_name = os.path.basename(resource['path'])
        if file_name in nuevos_paths:
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
    layer_name, workspace, base_url, wms_server_tiff, wms_layer_tiff, wms_description_tiff,url_new, wms_wfs = publish_to_geoserver(archivos)


    #integramos con geonetwork
    xml_data = generate_dynamic_xml(json_content, layer_name, workspace, base_url, uuid_key, coordenadas_tif, wms_server_tiff, wms_layer_tiff, wms_description_tiff, id_mission, url_new,  ruta_png, ruta_pdf, ruta_csv, ruta_tiff, zip_file_save, wms_wfs)

    resources_id = upload_to_geonetwork_xml([xml_data])
    upload_tiff_attachment(resources_id, xml_data, archivos)

    archivo_pdf = next((a for a in archivos if a["file_name"].lower().endswith(".pdf")), None)
    if not archivo_pdf:
        raise Exception("No se encontró archivo PDF para referenciar")
    
    archivo_tiff = next((a for a in archivos if a["file_name"].lower().endswith(".tif")), None)
    if not archivo_tiff:
        raise Exception("No se encontró archivo tif para referenciar")





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

def obtener_coordenadas_tif(name, content):
    with rasterio.open(io.BytesIO(content)) as dataset:
        bounds = dataset.bounds  
        coordenadas = {
            "min_longitud": bounds.left,
            "max_longitud": bounds.right,
            "min_latitud": bounds.bottom,
            "max_latitud": bounds.top
        }
    
    return coordenadas

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


# Definición del DAG
default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': task_failure_callback
}

dag = DAG(
    'water_analysis',
    default_args=default_args,
    description='DAG que procesa analisis de aguas',
    schedule_interval=None,
    catchup=False,
    concurrency=5,
    max_active_runs=5
)
process_extracted_files_task = PythonOperator(
    task_id='process_extracted_files_task',
    python_callable=process_extracted_files,
    provide_context=True,
    dag=dag,
)

process_extracted_files_task 