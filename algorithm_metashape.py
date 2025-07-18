import base64
import logging
import os
import tempfile
import xml
import zipfile
import pytz
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from dag_utils import get_minio_client, execute_query, get_geoserver_connection, download_from_minio
from airflow.models import Variable
import uuid
import io
import json
from airflow.hooks.base import BaseHook
import requests
from xml.sax.saxutils import escape

def process_json(**kwargs):
    otros = kwargs['dag_run'].conf.get('otros', [])
    json_content_original = kwargs['dag_run'].conf.get('json')

    if not json_content_original:
        print("Ha habido un error con el traspaso de los documentos")
        return
    print("Archivos para procesar preparados")

    s3_client = get_minio_client()
    file_path_in_minio = otros.replace('tmp/', '')
    folder_prefix = 'sftp/'

    try:
        local_zip_path = download_from_minio(s3_client, 'tmp', file_path_in_minio, 'tmp', folder_prefix)
        print(f"ZIP descargado: {local_zip_path}")
    except Exception as e:
        print(f"Error al descargar desde MinIO: {e}")
        raise

    if local_zip_path is None or not os.path.exists(local_zip_path):
        print(f"Archivo no encontrado: {local_zip_path}")
        return

    try:
        with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
            zip_file.testzip()
            print("El archivo ZIP es válido.")
    except zipfile.BadZipFile:
        print("El archivo no es un ZIP válido antes del procesamiento.")
        return

    with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
        with tempfile.TemporaryDirectory() as temp_dir:
            print(f"Directorio temporal creado: {temp_dir}")
            zip_file.extractall(temp_dir)
            file_list = zip_file.namelist()
            print("Archivos en el ZIP:", file_list)

            updated_json = generateSimplifiedCopy(json_content_original)
            

            id_mission = next(
                (item['value'] for item in updated_json['metadata'] if item['name'] == 'MissionID'),
                None
            )
            print("ID de misión encontrado:", id_mission)

            # Obtener nombre de misión desde base de datos
            mission_name_query = f"SELECT name FROM missions.mss_mission WHERE id = {id_mission}"
            try:
                mission_name_result = execute_query('biobd', mission_name_query)
                mission_name_raw = mission_name_result[0][0] if mission_name_result and len(mission_name_result) > 0 else f"Mission{id_mission}"
                mission_name_clean = sanitize_mission_name(mission_name_raw)
                print(f"Nombre de misión sanitizado: {mission_name_clean}")
            except Exception as e:
                print(f"Error al obtener nombre de misión: {e}")
                mission_name_clean = f"Mission{id_mission}"

                        # Pasamos el data capture a metadata
            orto_resource = next(
                (resource for resource in json_content_original["executionResources"]
                if "ortomosaico" in resource["path"].lower()),
                None
            )

            if orto_resource:
                data_capture_value = next(
                    (item["value"] for item in orto_resource["data"] if item["name"] == "dataCapture"),
                    None
                )
                if data_capture_value:
                    updated_json["dataCapture"] = data_capture_value
                print(f"Data Capture encontrado y modificado en metadata: {data_capture_value}")



            startTimeStamp = updated_json['startTimestamp']
            endTimeStamp = updated_json['endTimestamp']
            print("startTimeStamp:", startTimeStamp)
            print("endTimeStamp:", endTimeStamp)

            uuid_key = str(uuid.uuid4())
            bucket_name = 'missions'
            json_key = f"{id_mission}/{uuid_key}/algorithm_result.json"
            print(json_key)
            ruta_minio = Variable.get("ruta_minIO")

            # Subir el JSON original
            s3_client.put_object(
                Bucket=bucket_name,
                Key=json_key,
                Body=io.BytesIO(json.dumps(json_content_original).encode('utf-8')),
                ContentType='application/json'
            )
            print(f'Archivo JSON original subido correctamente a MinIO como {json_key}')

            # Crear índices de archivos y carpetas extraídas
            file_lookup = {}
            dir_lookup = {}
            publish_files = []
            images_files = []

            for root, dirs, files in os.walk(temp_dir):
                for dir_name in dirs:
                    local_dir_path = os.path.join(root, dir_name)
                    relative_dir_path = os.path.relpath(local_dir_path, temp_dir)
                    parts = relative_dir_path.split(os.sep)
                    if parts[0].lower().startswith("metashape"):
                        relative_dir_path = os.path.join(*parts[1:]) if len(parts) > 1 else parts[0]
                    dir_lookup[dir_name] = relative_dir_path

                for file_name in files:
                    if file_name.lower() == 'algorithm_result.json':
                        continue
                    local_path = os.path.join(root, file_name)
                    relative_path = os.path.relpath(local_path, temp_dir)
                    parts = relative_path.split(os.sep)
                    if parts[0].lower().startswith("metashape"):
                        relative_path = os.path.join(*parts[1:]) if len(parts) > 1 else parts[0]
                    file_lookup[file_name] = relative_path

            # Actualizar rutas en el JSON usando el índice real
            for resource in updated_json.get("executionResources", []):
                old_path = resource['path']
                file_name = os.path.basename(old_path)
                relative_path = file_lookup.get(file_name)

                if not relative_path:
                    relative_path = dir_lookup.get(file_name)

                if not relative_path:
                    print(f"No se encontró {file_name} en el ZIP extraído.")
                    continue

                new_path = f"/{bucket_name}/{id_mission}/{uuid_key}/{relative_path}"
                full_path = f"{ruta_minio.rstrip('/')}{new_path}"
                resource['path'] = full_path
                print(f"Ruta actualizada: {old_path} -> {full_path}")

            # Subir archivos del ZIP a MinIO
            for root, dirs, files in os.walk(temp_dir):
                for file_name in files:
                    if file_name.lower() == 'algorithm_result.json':
                        continue
                    local_path = os.path.join(root, file_name)
                    relative_path = os.path.relpath(local_path, temp_dir)
                    parts = relative_path.split(os.sep)
                    if parts[0].lower().startswith("metashape"):
                        relative_path = os.path.join(*parts[1:]) if len(parts) > 1 else parts[0]
                    minio_key = f"{id_mission}/{uuid_key}/{relative_path}"
                    try:
                        with open(local_path, 'rb') as file_data:
                            file_bytes = file_data.read()
                            
                            s3_client.put_object(
                                Bucket=bucket_name,
                                Key=minio_key,
                                Body=io.BytesIO(file_bytes),
                                ContentType="image/tiff" if relative_path.endswith(('.tif', '.tiff')) else 'application/octet-stream'
                            )

                            print(f"Archivo subido a MinIO: {minio_key}")                           
                    except Exception as e:
                        print(f"Error al subir {file_name} a MinIO: {e}")

                    if len(parts) == 2 and parts[0].lower() == 'resources':
                        publish_files.append({
                            "file_name": file_name,
                            "content": base64.b64encode(file_bytes).decode('utf-8')
                        })
                        if file_name.lower().endswith(('.png', '.jpg', '.jpeg')):
                            ruta_minio_archivo = f"{ruta_minio}/{bucket_name}/{minio_key}"
                            print(f"Ruta MinIO para imagen: {ruta_minio_archivo}")
                            images_files.append({
                               "file_name": file_name,
                               "content": ruta_minio_archivo
                            })

            # Subir el JSON original
            json_key_actualizado = f"{id_mission}/{uuid_key}/algorithm_result_actualizado.json"
            s3_client.put_object(
                Bucket=bucket_name,
                Key=json_key_actualizado,
                Body=io.BytesIO(json.dumps(updated_json).encode('utf-8')),
                ContentType='application/json'
            )
            print(f'Archivo JSON actualizado subido correctamente a MinIO como {json_key}')

            # Historizar
            historizacion(json_content_original, updated_json, id_mission, startTimeStamp, endTimeStamp)

            wms_layers_info = publish_to_geoserver(publish_files)

            import time
            time.sleep(5)


            bbox = extract_bbox_from_json(json_content_original)
            print(bbox)

            #integramos con geonetwork
            xml_data = generate_dynamic_xml(updated_json, bbox, uuid_key, id_mission, wms_layers_info, images_files, json_content_original, mission_name_clean)
            resources_id = upload_to_geonetwork_xml([xml_data])
            print(resources_id)



def get_geonetwork_credentials():
    try:

        conn = BaseHook.get_connection('geonetwork_credentials')
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
    


def generateSimplifiedCopy(json_content_original):      
        updated_json = json.loads(json.dumps(json_content_original))
        keys_a_mantener = {'identifier', 'pixelSize'}
        updated_json.pop("executionArguments", None)
        for resource in updated_json.get("executionResources", []):
            filtered_data = []
            for item in resource.get("data", []):
                if item["name"] in keys_a_mantener:
                    filtered_data.append(item)
            resource["data"] = filtered_data
        updated_json["executionResources"] = [
            res for res in updated_json["executionResources"] if res.get("output", False)
        ]
        print(updated_json)
        return updated_json



def extract_bbox_from_json(json_data):
    """
    Extrae el diccionario de coordenadas del BBOX desde cualquier recurso que lo contenga.
    Prioriza recursos con output=false (metadatos de ráfaga), pero busca en todos si es necesario.
    """
    # Primero buscar en recursos con output=false (metadatos)
    for resource in json_data.get("executionResources", []):
        if not resource.get("output", True):  # output=false o no definido
            for item in resource.get("data", []):
                if item.get("name") == "BBOX" and item.get("value"):
                    print(f"BBOX encontrado en recurso de metadatos: {resource.get('path')}")
                    return item["value"]
    
    # Si no se encuentra, buscar en todos los recursos
    for resource in json_data.get("executionResources", []):
        for item in resource.get("data", []):
            if item.get("name") == "BBOX" and item.get("value"):
                print(f"BBOX encontrado en recurso: {resource.get('path')}")
                return item["value"]
    
    raise ValueError("No se encontró el BBOX en ningún recurso del JSON.")



#HISTORIZACION
def historizacion(input_data, output_data, mission_id, startTimeStamp, endTimeStamp):
    try:
        # Y guardamos en la tabla de historico
            print("Guardamos en historización/metashape")
            madrid_tz = pytz.timezone('Europe/Madrid')

            # Formatear phenomenon_time
            start_dt = datetime.strptime(startTimeStamp, "%Y%m%dT%H%M%S")
            end_dt = datetime.strptime(endTimeStamp, "%Y%m%dT%H%M%S")

            phenomenon_time = f"[{start_dt.strftime('%Y-%m-%dT%H:%M:%S')}, {end_dt.strftime('%Y-%m-%dT%H:%M:%S')}]"
            datos = {
                'sampled_feature': mission_id,
                'result_time': datetime.now(madrid_tz),
                'phenomenon_time': phenomenon_time,
                'input_data': json.dumps(input_data), #rutas antiguas
                'output_data': json.dumps(output_data) #JSON MODIFICADOOO CON LO NUEVO
                # se crea un json apartir del antiguo con las rutas nuevas en minio
            }

            # Construir la consulta de inserción
            query = f"""
                INSERT INTO algoritmos.algoritmo_metashape (
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
    WORKSPACE = "MetashapeResult"
    GENERIC_LAYER = "algoritm_metashape_result"

    if not archivos:
        raise Exception("No hay archivos para subir a GeoServer.")
    
    # Subida a GeoServer
    base_url, auth = get_geoserver_connection("geoserver_connection")
    temp_files = []
    
    for archivo in archivos:
        archivo_file_name = archivo['file_name']
        archivo_content = base64.b64decode(archivo['content'])

        archivo_file_name = os.path.basename(archivo_file_name)
        archivo_extension = os.path.splitext(archivo_file_name)[1]

        nombre_base = os.path.splitext(archivo_file_name)[0]
        temp_file_path = os.path.join("/tmp", f"{nombre_base}{archivo_extension}")

        # Guardar el archivo en el sistema antes de usarlo
        with open(temp_file_path, 'wb') as temp_file:
            temp_file.write(archivo_content)

        temp_files.append((archivo_file_name, temp_file_path))
   

    #Seleccionamos los tiff
    tiff_files = [path for name, path in temp_files if name.lower().endswith(('.tif', '.tiff'))]
    wms_layers_info = []
    wms_server_tiff = None
    wms_layer_tiff = None
    wms_description_tiff = "Capa publicada en GeoServer"

    
    for tif_file in tiff_files:
        with open(tif_file, 'rb') as f:
            file_data = f.read()

        unique_suffix = uuid.uuid4().hex[:6]
        layer_name = f"{GENERIC_LAYER}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{unique_suffix}"
        headers = {"Content-type": "image/tiff"}

        # Publicar capa raster en GeoServer
        url_new = f"{base_url}/workspaces/{WORKSPACE}/coveragestores/{layer_name}/file.geotiff"
        response = requests.put(url_new, headers=headers, data=file_data, auth=auth, params={"configure": "all"})
        if response.status_code not in [201, 202]:
            raise Exception(f"Error publicando {layer_name}: {response.text}")
        print(f"Capa raster publicada: {layer_name}")

        wms_server_tiff = f"{base_url}/{WORKSPACE}/wms"
        wms_layer_tiff = f"{WORKSPACE}:{layer_name}"
        wms_layers_info.append({
            "wms_server": wms_server_tiff,
            "wms_layer": wms_layer_tiff,
            "layer_name": layer_name,
            "wms_description_tiff" : wms_description_tiff
        })
        try:
            # Actualizar capa genérica
            url_latest = f"{base_url}/workspaces/{WORKSPACE}/coveragestores/{GENERIC_LAYER}/file.geotiff"
            response_latest = requests.put(url_latest, headers=headers, data=file_data, auth=auth, params={"configure": "all"})
            if response_latest.status_code not in [201, 202]:
                raise Exception(f"Error actualizando capa genérica: {response_latest.text}")
            print(f"Capa genérica raster actualizada: {GENERIC_LAYER}")
            print(f"Raster disponible en: {base_url}/geoserver/{WORKSPACE}/wms?layers={WORKSPACE}:{layer_name}")
        except Exception as e:
            print(f"Capa generica ya levantada: {e}")

    return wms_layers_info





def upload_to_geonetwork_xml(xml_data_array):
        try:
            connection = BaseHook.get_connection("geonetwork_connection")
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
                params = {
                    "uuidProcessing": "OVERWRITE"
                }

                response = requests.post(upload_url, files=files, headers=headers , params=params)
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


def generate_dynamic_xml(json_modificado, bbox, uuid_key, id_mission, wms_layers_info, images_files, json_original, mission_name):

    fecha_completa = datetime.strptime(json_modificado['endTimestamp'], "%Y%m%dT%H%M%S")
    fecha = fecha_completa.date()

    publication_date = datetime.now().strftime("%Y-%m-%d")

    # Obtener AlgorithmID y generar título según especificaciones
    algorithm_id = next(
        (item['value'] for item in json_modificado['metadata'] if item['name'] == 'AlgorithmID'),
        'MetashapeRGB'  # valor por defecto
    )

    # Obtener recurso principal (esto debe ir ANTES de usar orto_data)
    orto_data = next(
        (res for res in json_original['executionResources']
            if 'ortomosaico' in res['path'].lower()),
        None
    )

    if not orto_data:
        raise ValueError("No se encontró el recurso Ortomosaico_Temp.tif en el JSON.")

    # Obtener fecha de captura y formatear (YYYYMMDD)
    data_capture = next(
        (item['value'] for item in orto_data['data'] if item['name'] == 'dataCapture'),
        datetime.now().strftime('%Y%m%dT%H%M')
    )
    fecha_captura = data_capture[:8]  # Tomar solo YYYYMMDD

    # Generar título según AlgorithmID
    if algorithm_id == 'MetashapeRGB':
        titulo = f"CartoRGB_{mission_name}_{fecha_captura}"
    elif algorithm_id == 'MetashapeIR':
        titulo = f"CartoIR_{mission_name}_{fecha_captura}"
    elif algorithm_id == 'MetashapeMULTI':
        titulo = f"CartoMulti_{mission_name}_{fecha_captura}"
    else:
        titulo = f"Metashape_{mission_name}_{fecha_captura}"  # fallback

    print(f"Título generado: {titulo}")

    graphic_overview_xml = generar_graphic_overview(images_files)
    gmd_online_resources = generar_gmd_online(wms_layers_info, json_original, algorithm_id)

    # Consolidar metadatos de todas las capas
    consolidated_metadata = consolidate_metadata_from_all_layers(json_original)

    metadata_dict = {item['name']: item['value'] for item in orto_data['data']}
    tags = consolidated_metadata["tags"]
    
    tipo_representacion = metadata_dict.get("SpatialRepresentationTypeCode", "grid")
    reference_system = next((item['value'] for item in json_modificado['metadata']
                            if item['name'] == "ReferenceSystem"), "EPSG:4326")


    inspire_themes = consolidated_metadata["inspire_themes"]
    topic_category = consolidated_metadata["topic_categories"]
    
    inspire_theme_list = [theme.strip() for theme in inspire_themes.split(",") if theme.strip()]

    # Generar descripción según AlgorithmID
    if algorithm_id == 'MetashapeRGB':
        descripcion = ("Conjunto de datos geoespaciales generado a partir de un vuelo "
                    "fotogramétrico, que incluye productos derivados del procesamiento de "
                    "imágenes aéreas. Este conjunto puede contener ortomosaico RGB, modelo "
                    "digital de superficie (MDS), modelo digital del terreno (MDT) y nubes de "
                    "puntos. La disponibilidad de cada producto puede variar en función de la "
                    "zona de vuelo y las condiciones de captura establecidas")
                    
    elif algorithm_id == 'MetashapeIR':
        descripcion = ("Conjunto de datos geoespaciales generado a partir de un vuelo "
                    "fotogramétrico, que incluye productos derivados del procesamiento de "
                    "imágenes aéreas. Este conjunto puede contener ortomosaico IR, modelo "
                    "digital de superficie (MDS), modelo digital del terreno (MDT). La "
                    "disponibilidad de cada producto puede variar en función de la zona de "
                    "vuelo y las condiciones de captura establecidas")
                    
    elif algorithm_id == 'MetashapeMULTI':
        descripcion = ("Conjunto de datos geoespaciales generado a partir de un vuelo "
                    "fotogramétrico con sensor multiespectral, que incluye productos "
                    "derivados del procesamiento de imágenes aéreas. Este conjunto puede "
                    "contener ortomosaicos multiespectrales, modelos digitales de superficie "
                    "(MDS), modelos digitales del terreno (MDT), nubes de puntos clasificadas "
                    "y distintos índices multiespectrales como NDVI, SAVI, EVI... La "
                    "disponibilidad de cada producto puede variar en función de la zona de "
                    "vuelo, el sensor empleado y los objetivos del levantamiento")
    else:
        # Fallback: usar specificUsage del recurso principal
        descripcion = next(
            (item["value"] for item in orto_data["data"] if item["name"] == "specificUsage"),
            "Conjunto de datos geoespaciales generado mediante procesamiento fotogramétrico"
        )

    print(f"Descripción generada para {algorithm_id}: {descripcion}")

    print(f"Fecha de publicación: {publication_date}")
    print(f"Etiquetas: {tags}")
    print(f"Inspire Themes: {inspire_themes}")
    print(f"Categoría: {topic_category}")
    print(f"Sistema de referencia: {reference_system}")
    print(f"Tipo de representación espacial: {tipo_representacion}")
    print(f"Resolución espacial: {metadata_dict.get('pixelSize', '0.0')}")


    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
    <gmd:MD_Metadata 
        xmlns:gmd="http://www.isotc211.org/2005/gmd"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns:gco="http://www.isotc211.org/2005/gco"
        xmlns:srv="http://www.isotc211.org/2005/srv"
        xmlns:gmx="http://www.isotc211.org/2005/gmx"
        xmlns:gts="http://www.isotc211.org/2005/gts"
        xmlns:gsr="http://www.isotc211.org/2005/gsr"
        xmlns:gmi="http://www.isotc211.org/2005/gmi"
        xmlns:gml="http://www.opengis.net/gml/3.2"
        xmlns:xlink="http://www.w3.org/1999/xlink"
        xsi:schemaLocation="http://www.isotc211.org/2005/gmd 
                            http://schemas.opengis.net/csw/2.0.2/profiles/apiso/1.0.0/apiso.xsd">

                            
        <gmd:language>
            <gmd:LanguageCode codeList="http://www.loc.gov/standards/iso639-2/" codeListValue="es"/>
        </gmd:language>

        <gmd:characterSet>
            <gmd:MD_CharacterSetCode codeListValue="utf8"
                                codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_CharacterSetCode"/>
        </gmd:characterSet>

        <gmd:hierarchyLevel>
            <gmd:MD_ScopeCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_ScopeCode"
                                codeListValue="dataset"/>
        </gmd:hierarchyLevel>


        <gmd:contact>
            <gmd:CI_ResponsibleParty>
                <gmd:individualName>
                    <gco:CharacterString>I+D</gco:CharacterString>
                </gmd:individualName>
                <gmd:organisationName>
                    <gco:CharacterString>Avincis</gco:CharacterString>
                </gmd:organisationName>
                <gmd:contactInfo>
                    <gmd:CI_Contact>
                    <gmd:address>
                        <gmd:CI_Address>
                            <gmd:electronicMailAddress>
                                <gco:CharacterString>soporte@einforex.es</gco:CharacterString>
                            </gmd:electronicMailAddress>
                        </gmd:CI_Address>
                    </gmd:address>
                    <gmd:onlineResource>
                        <gmd:CI_OnlineResource>
                            <gmd:linkage>
                                <gmd:URL>https://www.avincis.com</gmd:URL>
                            </gmd:linkage>
                            <gmd:protocol gco:nilReason="missing">
                                <gco:CharacterString/>
                            </gmd:protocol>
                            <gmd:name gco:nilReason="missing">
                                <gco:CharacterString/>
                            </gmd:name>
                            <gmd:description gco:nilReason="missing">
                                <gco:CharacterString/>
                            </gmd:description>
                        </gmd:CI_OnlineResource>
                    </gmd:onlineResource>
                    </gmd:CI_Contact>
                </gmd:contactInfo>
                <gmd:role>
                    <gmd:CI_RoleCode codeListValue="author"
                                    codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_RoleCode"/>
                </gmd:role>
            </gmd:CI_ResponsibleParty>
        </gmd:contact>

        
        <gmd:dateStamp>
            <gco:DateTime>${fecha_completa}</gco:DateTime>
        </gmd:dateStamp>

        <gmd:metadataStandardName>
            <gco:CharacterString>ISO 19115:2003/19139</gco:CharacterString>
        </gmd:metadataStandardName>


        <gmd:metadataStandardVersion>
            <gco:CharacterString>1.0</gco:CharacterString>
        </gmd:metadataStandardVersion>



        <gmd:referenceSystemInfo>
            <gmd:MD_ReferenceSystem>
                <gmd:referenceSystemIdentifier>
                    <gmd:RS_Identifier>
                        <gmd:code>
                            <gco:CharacterString>{reference_system}</gco:CharacterString>
                        </gmd:code>
                        <gmd:codeSpace>
                            <gco:CharacterString>EPSG</gco:CharacterString>
                        </gmd:codeSpace>
                    </gmd:RS_Identifier>
                </gmd:referenceSystemIdentifier>
            </gmd:MD_ReferenceSystem>
        </gmd:referenceSystemInfo>


        <gmd:identificationInfo>
            <srv:SV_ServiceIdentification>
                <gmd:citation>
                    <gmd:CI_Citation>
                    <gmd:title>
                        <gco:CharacterString>{titulo}</gco:CharacterString>
                    </gmd:title>
                    <gmd:date>
                        <gmd:CI_Date>
                            <gmd:date>
                                <gco:Date>${fecha}</gco:Date>
                            </gmd:date>
                            <gmd:dateType>
                                <gmd:CI_DateTypeCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_DateTypeCode"
                                                    codeListValue="publication"/>
                            </gmd:dateType>
                        </gmd:CI_Date>
                    </gmd:date>
                    </gmd:CI_Citation>
                </gmd:citation>

                {graphic_overview_xml}

                <gmd:abstract>
                    <gco:CharacterString>{descripcion}</gco:CharacterString>
                </gmd:abstract>
                <gmd:status>
                    <gmd:MD_ProgressCode codeListValue="completed"
                                        codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_ProgressCode"/>
                </gmd:status>
                <gmd:pointOfContact>
                    <gmd:CI_ResponsibleParty>
                    <gmd:individualName>
                        <gco:CharacterString>I+D</gco:CharacterString>
                    </gmd:individualName>
                    <gmd:organisationName>
                        <gco:CharacterString>Avincis</gco:CharacterString>
                    </gmd:organisationName>
                    <gmd:contactInfo>
                        <gmd:CI_Contact>
                            <gmd:address>
                                <gmd:CI_Address>
                                <gmd:electronicMailAddress>
                                    <gco:CharacterString>soporte@einforex.es</gco:CharacterString>
                                </gmd:electronicMailAddress>
                                </gmd:CI_Address>
                            </gmd:address>
                            <gmd:onlineResource>
                                <gmd:CI_OnlineResource>
                                <gmd:linkage>
                                    <gmd:URL>https://www.avincis.com</gmd:URL>
                                </gmd:linkage>
                                <gmd:protocol gco:nilReason="missing">
                                    <gco:CharacterString/>
                                </gmd:protocol>
                                <gmd:name gco:nilReason="missing">
                                    <gco:CharacterString/>
                                </gmd:name>
                                <gmd:description gco:nilReason="missing">
                                    <gco:CharacterString/>
                                </gmd:description>
                                </gmd:CI_OnlineResource>
                            </gmd:onlineResource>
                        </gmd:CI_Contact>
                    </gmd:contactInfo>
                    <gmd:role>
                        <gmd:CI_RoleCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_RoleCode"
                                        codeListValue="author"/>
                    </gmd:role>
                    </gmd:CI_ResponsibleParty>
                </gmd:pointOfContact>


                <gmd:citation>
                    <gmd:CI_Citation>
                        <gmd:date>
                            <gmd:CI_Date>
                                <gmd:date>
                                    <gco:Date>{publication_date}</gco:Date>
                                </gmd:date>
                                <gmd:dateType>
                                    <gmd:CI_DateTypeCode codeList="http://www.isotc211.org/2005/resources/codeList.xml#CI_DateTypeCode"
                                                        codeListValue="publication">Publicación</gmd:CI_DateTypeCode>
                                </gmd:dateType>
                            </gmd:CI_Date>
                        </gmd:date>
                    </gmd:CI_Citation>
                </gmd:citation>

                <gmd:resourceMaintenance>
                    <gmd:MD_MaintenanceInformation>
                        <gmd:maintenanceAndUpdateFrequency>
                            <gmd:MD_MaintenanceFrequencyCode codeList="http://www.isotc211.org/2005/resources/codeList.xml#MD_MaintenanceFrequencyCode"
                                                            codeListValue="continual">Continualmente</gmd:MD_MaintenanceFrequencyCode>
                        </gmd:maintenanceAndUpdateFrequency>
                        <gmd:maintenanceAndUpdateFrequency>
                            <gmd:MD_MaintenanceFrequencyCode codeList="http://www.isotc211.org/2005/resources/codeList.xml#MD_MaintenanceFrequencyCode"
                                                            codeListValue="notPlanned">Sin planificar</gmd:MD_MaintenanceFrequencyCode>
                        </gmd:maintenanceAndUpdateFrequency>
                    </gmd:MD_MaintenanceInformation>
                </gmd:resourceMaintenance>

                <gmd:descriptiveKeywords>
                    <gmd:MD_Keywords>
                        {"".join(f'<gmd:keyword><gco:CharacterString>{tag.strip()}</gco:CharacterString></gmd:keyword>' for tag in tags)}
                        <gmd:thesaurusName>
                        <gmd:CI_Citation>
                            <gmd:title>
                                <gco:CharacterString>Palabras clave</gco:CharacterString>
                            </gmd:title>
                        </gmd:CI_Citation>
                    </gmd:thesaurusName>
                    </gmd:MD_Keywords>
                </gmd:descriptiveKeywords>
                
            <gmd:descriptiveKeywords>
                <gmd:MD_Keywords>
                    {"".join(f'<gmd:keyword><gco:CharacterString>{theme}</gco:CharacterString></gmd:keyword>' for theme in inspire_theme_list)}
                    <gmd:thesaurusName>
                        <gmd:CI_Citation>
                            <gmd:title>
                                <gco:CharacterString>Temas INSPIRE</gco:CharacterString>
                            </gmd:title>
                        </gmd:CI_Citation>
                    </gmd:thesaurusName>
                </gmd:MD_Keywords>
            </gmd:descriptiveKeywords>

            <gmd:language>
                <gmd:LanguageCode codeList="http://www.loc.gov/standards/iso639-2/" 
                                codeListValue="spa">spa</gmd:LanguageCode>
            </gmd:language>

            <gmd:descriptiveKeywords>
                <gmd:MD_Keywords>
                    {"".join(f'<gmd:keyword><gco:CharacterString>{category.strip()}</gco:CharacterString></gmd:keyword>'
                            for category in topic_category.split(","))}
                    <gmd:thesaurusName>
                        <gmd:CI_Citation>
                            <gmd:title>
                                <gco:CharacterString>Categorías</gco:CharacterString>
                            </gmd:title>
                        </gmd:CI_Citation>
                    </gmd:thesaurusName>
                </gmd:MD_Keywords>
            </gmd:descriptiveKeywords>

            <gmd:spatialRepresentationType>
                <gmd:MD_SpatialRepresentationTypeCode 
                    codeList="http://www.isotc211.org/2005/resources/codeList.xml#MD_SpatialRepresentationTypeCode"
                    codeListValue="{tipo_representacion}">{tipo_representacion}</gmd:MD_SpatialRepresentationTypeCode>
            </gmd:spatialRepresentationType>

            <gmd:spatialResolution>
                <gmd:MD_Resolution>
                    <gmd:distance>
                        <gco:Distance uom="m">{metadata_dict.get("pixelSize", "0.0")}</gco:Distance>
                    </gmd:distance>
                </gmd:MD_Resolution>
            </gmd:spatialResolution>


                <srv:serviceType>
                    <gco:LocalName codeSpace="www.w3c.org">OGC:WMS</gco:LocalName>
                </srv:serviceType>
                <srv:serviceTypeVersion>
                    <gco:CharacterString>1.3.0</gco:CharacterString>
                </srv:serviceTypeVersion>
                <srv:extent>
                    <gmd:EX_Extent xmlns:xs="http://www.w3.org/2001/XMLSchema">
                    <gmd:geographicElement>
                        <gmd:EX_GeographicBoundingBox>
                        <gmd:westBoundLongitude>
                            <gco:Decimal>{bbox['westBoundLongitude']}</gco:Decimal>
                        </gmd:westBoundLongitude>
                        <gmd:eastBoundLongitude>
                            <gco:Decimal>{bbox['eastBoundLongitude']}</gco:Decimal>
                        </gmd:eastBoundLongitude>
                        <gmd:southBoundLatitude>
                            <gco:Decimal>{bbox['southBoundLatitude']}</gco:Decimal>
                        </gmd:southBoundLatitude>
                        <gmd:northBoundLatitude>
                            <gco:Decimal>{bbox['northBoundLatitude']}</gco:Decimal>
                        </gmd:northBoundLatitude>
                        </gmd:EX_GeographicBoundingBox>
                    </gmd:geographicElement>
                    </gmd:EX_Extent>
                </srv:extent>

                <gmd:presentationForm>
                    <gco:CharacterString>Modelo Digital</gco:CharacterString>
                </gmd:presentationForm>

                <srv:couplingType>
                    <srv:SV_CouplingType codeListValue="tight"
                                        codeList="http://www.isotc211.org/2005/iso19119/resources/Codelist/gmxCodelists.xml#SV_CouplingType"/>
                </srv:couplingType>


            </srv:SV_ServiceIdentification>
        </gmd:identificationInfo>
                
        
        <gmd:distributionInfo>
            <gmd:MD_Distribution>
                <gmd:distributor>
                    <gmd:MD_Distributor>
                    <gmd:distributorContact>
                        <gmd:CI_ResponsibleParty>
                            <gmd:individualName>
                                <gco:CharacterString>I+D</gco:CharacterString>
                            </gmd:individualName>
                            <gmd:organisationName>
                                <gco:CharacterString>Avincis</gco:CharacterString>
                            </gmd:organisationName>
                            <gmd:contactInfo>
                                <gmd:CI_Contact>
                                <gmd:address>
                                    <gmd:CI_Address>
                                        <gmd:electronicMailAddress>
                                            <gco:CharacterString>soporte@einforex.es</gco:CharacterString>
                                        </gmd:electronicMailAddress>
                                    </gmd:CI_Address>
                                </gmd:address>
                                <gmd:onlineResource>
                                    <gmd:CI_OnlineResource>
                                        <gmd:linkage>
                                            <gmd:URL>https://www.avincis.com</gmd:URL>
                                        </gmd:linkage>
                                        <gmd:protocol gco:nilReason="missing">
                                            <gco:CharacterString/>
                                        </gmd:protocol>
                                        <gmd:name gco:nilReason="missing">
                                            <gco:CharacterString/>
                                        </gmd:name>
                                        <gmd:description gco:nilReason="missing">
                                            <gco:CharacterString/>
                                        </gmd:description>
                                    </gmd:CI_OnlineResource>
                                </gmd:onlineResource>
                                </gmd:CI_Contact>
                            </gmd:contactInfo>
                            <gmd:role>
                                <gmd:CI_RoleCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_RoleCode"
                                                codeListValue="distributor"/>
                            </gmd:role>
                        </gmd:CI_ResponsibleParty>
                    </gmd:distributorContact>
                    </gmd:MD_Distributor>
                </gmd:distributor>
                
                <gmd:transferOptions>
                    <gmd:MD_DigitalTransferOptions>
                      {gmd_online_resources}
                    </gmd:MD_DigitalTransferOptions>
                </gmd:transferOptions>

                
            </gmd:MD_Distribution>
        </gmd:distributionInfo>
        

        <gmd:dataQualityInfo>
                <gmd:DQ_DataQuality>
                    <gmd:scope>
                        <gmd:DQ_Scope>
                        <gmd:level>
                            <gmd:MD_ScopeCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_ScopeCode"
                                                codeListValue="service"/>
                        </gmd:level>
                        </gmd:DQ_Scope>
                    </gmd:scope>
                    <gmd:lineage>
                        <gmd:LI_Lineage/>
                    </gmd:lineage>
                </gmd:DQ_DataQuality>
        </gmd:dataQualityInfo>
        <gmd:applicationSchemaInfo>
                <gmd:MD_ApplicationSchemaInformation>
                    <gmd:name>
                        <gmd:CI_Citation>
                        <gmd:title gco:nilReason="missing">
                            <gco:CharacterString/>
                        </gmd:title>
                        <gmd:date>
                            <gmd:CI_Date>
                                <gmd:date>
                                    <gco:Date/>
                                </gmd:date>
                                <gmd:dateType>
                                    <gmd:CI_DateTypeCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_DateTypeCode"
                                                        codeListValue=""/>
                                </gmd:dateType>
                            </gmd:CI_Date>
                        </gmd:date>
                        </gmd:CI_Citation>
                    </gmd:name>
                    <gmd:schemaLanguage gco:nilReason="missing">
                        <gco:CharacterString/>
                    </gmd:schemaLanguage>
                    <gmd:constraintLanguage gco:nilReason="missing">
                        <gco:CharacterString/>
                    </gmd:constraintLanguage>
                </gmd:MD_ApplicationSchemaInformation>
            </gmd:applicationSchemaInfo>
        </gmd:MD_Metadata>
"""
    xml_encoded = base64.b64encode(xml.encode('utf-8')).decode('utf-8')
    return xml_encoded

def generate_preview_url(wms, bbox, reference_system):
    """Genera URL de preview dinámicamente"""
    wms_base_url = wms['wms_server'].replace('/rest/', '/')
    
    if bbox:
        bbox_str = f"{bbox['westBoundLongitude']},{bbox['southBoundLatitude']},{bbox['eastBoundLongitude']},{bbox['northBoundLatitude']}"
        return f"{wms_base_url}?service=WMS&version=1.1.0&request=GetMap&layers={wms['wms_layer']}&bbox={bbox_str}&width=768&height=630&srs={reference_system}&styles=&format=application/openlayers"
    else:
        return f"{wms_base_url}?service=WMS&version=1.3.0&request=GetCapabilities"

def generar_gmd_online(wms_layers_info, json_original=None, algorithm_id=None):
    print(f"DEBUG generar_gmd_online: json_original disponible: {json_original is not None}")
    print(f"DEBUG generar_gmd_online: algorithm_id: {algorithm_id}")
    print(f"DEBUG generar_gmd_online: número de capas WMS: {len(wms_layers_info)}")

    online_blocks = ""
    
    for i, wms in enumerate(wms_layers_info):
        # Por defecto usar valores originales
        print(f"DEBUG capa {i}: layer_name = {wms.get('layer_name', 'NO_LAYER_NAME')}")
        display_name = wms['wms_layer']
        display_description = wms['wms_description_tiff']

        print(f"DEBUG capa {i}: display_name original = {display_name}")
        print(f"DEBUG capa {i}: display_description original = {display_description}")
        
        # Si tenemos datos para mapear, intentar mapear por orden
        if json_original and algorithm_id:
            # Obtener solo recursos output=true (archivos TIFF)
            output_resources = [res for res in json_original.get("executionResources", []) 
                            if res.get("output", False) and res.get("path", "").endswith(('.tif', '.tiff'))]
            
            # Mapear por orden (la capa i corresponde al recurso i)
            if i < len(output_resources):
                matching_resource = output_resources[i]
                mapped_name, mapped_desc = map_layer_name_and_description(matching_resource, algorithm_id)
                display_name = mapped_name
                display_description = mapped_desc
        print(f"DEBUG capa {i}: display_name FINAL = {display_name}")
        print(f"DEBUG capa {i}: display_description FINAL = {display_description[:100]}...")
        
        online_blocks += f"""
            <gmd:onLine>
                <gmd:CI_OnlineResource>
                    <gmd:linkage>
                        <gmd:URL>{escape(wms['wms_server'].replace('/rest/', '/'))}</gmd:URL>
                    </gmd:linkage>
                    <gmd:protocol>
                        <gco:CharacterString>OGC:WMS</gco:CharacterString>
                    </gmd:protocol>
                    <gmd:name>
                        <gco:CharacterString>{escape(wms['wms_layer'].split(':')[1])}</gco:CharacterString>
                    </gmd:name>
                    <gmd:description>
                        <gco:CharacterString>{escape(display_name)}: {escape(display_description)}</gco:CharacterString>
                    </gmd:description>
                    <gmd:function>
                        <gmd:CI_OnLineFunctionCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                codeListValue="download"/>
                    </gmd:function>
                </gmd:CI_OnlineResource>
            </gmd:onLine>
        """
    return online_blocks

def generar_graphic_overview(images_files):
    bloques = []
    for image in images_files:
        ruta_png = image["content"]
        bloque = f"""
        <gmd:graphicOverview>
            <gmd:MD_BrowseGraphic>
                <gmd:fileName>
                    <gco:CharacterString>{ruta_png}</gco:CharacterString>
                </gmd:fileName>
            </gmd:MD_BrowseGraphic>
        </gmd:graphicOverview>
        """
        bloques.append(bloque)
    return "\n".join(bloques)

def sanitize_mission_name(name):
    """
    Sanitiza el nombre de la misión eliminando espacios, acentos y caracteres especiales.
    Convierte a formato CamelCase.
    """
    import re
    import unicodedata
    
    # Eliminar acentos
    name = unicodedata.normalize('NFD', name)
    name = ''.join(char for char in name if unicodedata.category(char) != 'Mn')
    
    # Eliminar caracteres especiales y espacios, mantener solo letras, números
    name = re.sub(r'[^a-zA-Z0-9\s]', '', name)
    
    # Convertir a CamelCase
    words = name.split()
    camel_case = ''.join(word.capitalize() for word in words)
    
    return camel_case

def consolidate_metadata_from_all_layers(json_original):
    """
    Consolida metadatos técnicos de todas las capas con output: true.
    Elimina duplicados automáticamente.
    """
    all_inspire_themes = []
    all_topic_categories = []
    all_tags = []
    
    # Recorrer solo capas de salida (output: true)
    for resource in json_original.get("executionResources", []):
        if not resource.get("output", False):
            continue
            
        # Extraer metadatos de esta capa
        for item in resource.get("data", []):
            if item["name"] == "InspireThemes" and item.get("value"):
                themes = [theme.strip() for theme in item["value"].split(",")]
                all_inspire_themes.extend(themes)
                
            elif item["name"] == "TopicCategoryCode" and item.get("value"):
                categories = [cat.strip() for cat in item["value"].split(",")]
                all_topic_categories.extend(categories)
        
        # Extraer tags del resource
        if resource.get("tag"):
            tags = [tag.strip() for tag in resource["tag"].split(",")]
            all_tags.extend(tags)
    
    # Eliminar duplicados manteniendo orden
    unique_inspire_themes = list(dict.fromkeys(all_inspire_themes))
    unique_topic_categories = list(dict.fromkeys(all_topic_categories))
    unique_tags = list(dict.fromkeys(all_tags))
    
    return {
        "inspire_themes": ", ".join(unique_inspire_themes),
        "topic_categories": ", ".join(unique_topic_categories), 
        "tags": unique_tags
    }

def map_layer_name_and_description(resource, algorithm_id):
    """
    Mapea nombres de capas usando identifier + algorithmID.
    """
    # Obtener identifier
    identifier = next(
        (item['value'] for item in resource.get('data', []) if item['name'] == 'identifier'),
        os.path.basename(resource.get('path', ''))
    )
    
    # Obtener specificUsage
    specific_usage = next(
        (item['value'] for item in resource.get('data', []) if item['name'] == 'specificUsage'),
        None
    )
    
    filename_lower = identifier.lower()
    
    # Mapeo de nombres
    if 'ortomosaico' in filename_lower:
        if algorithm_id == 'MetashapeRGB':
            name = "Ortomosaico RGB"
            default_desc = "Ortoimagen generada a partir de imágenes aéreas en el espectro visible (RGB), ortorrectificadas y ensambladas en un mosaico continuo"
        elif algorithm_id == 'MetashapeIR':
            name = "Ortomosaico IR"
            default_desc = "Ortoimagen generada a partir de imágenes aéreas en el espectro infrarrojo"
        elif algorithm_id == 'MetashapeMULTI':
            name = "Ortomosaico Multibanda"
            default_desc = "Ortoimagen generada a partir de imágenes aéreas multiespectrales"
        else:
            name = identifier
            default_desc = "Ortoimagen generada a partir de imágenes aéreas"
            
    elif 'dem' in filename_lower:
        name = "DEM"
        default_desc = "Modelo digital que representa la superficie terrestre, incluyendo todas las estructuras sobre ella (edificios, árboles, etc.)"
        
    elif 'mdt' in filename_lower:
        name = "MDT"
        default_desc = "Modelo digital del relieve terrestre que incluye solo la superficie del suelo, sin elementos artificiales o vegetación"
        
    elif filename_lower.endswith('.las'):
        name = "Nubes de puntos RGB"
        default_desc = "Conjunto de puntos tridimensionales con información de color en formato RGB, generados mediante técnicas de fotogrametría a partir de imágenes aéreas superpuestas"
    else:
        name = identifier
        default_desc = "Capa generada mediante procesamiento fotogramétrico"
    
    # Usar specificUsage si existe, sino usar descripción por defecto
    description = specific_usage if specific_usage and specific_usage.strip() else default_desc
    
    return name, description


# Definición del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'retries': 1,
}

dag = DAG(
    'algorithm_metashape',
    default_args=default_args,
    description='DAG para analizar metashape',
    schedule_interval=None,  # Se puede ajustar según necesidades
    catchup=False
)

process_task = PythonOperator(
    task_id='process_task_metashape',
    python_callable=process_json, #Importante
    provide_context=True,
    dag=dag
)

# Definir el flujo de las tareas
process_task