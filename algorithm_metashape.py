import base64
import logging
import os
import tempfile
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
                                ContentType="image/tiff" if relative_path.endswith('.tif') else 'application/octet-stream'
                            )

                            print(f"Archivo subido a MinIO: {minio_key}")                           
                    except Exception as e:
                        print(f"Error al subir {file_name} a MinIO: {e}")

                    if len(parts) == 2 and parts[0].lower() == 'resources':
                        publish_files.append({
                            "file_name": file_name,
                            "content": base64.b64encode(file_bytes).decode('utf-8')
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

            print(f"archivos para publicar:")
            for f in publish_files:
                print(f"- {f['file_name']}")

            #wms_layers_info = publish_to_geoserver(publish_files)
            #print(wms_layers_info)

            bbox = extract_bbox_from_json(json_content_original)
            print(bbox)

            

            #integramos con geonetwork
            #xml_data = generate_dynamic_xml(updated_json, bbox, uuid_key, id_mission)



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
    Extrae el diccionario de coordenadas del BBOX desde el recurso Castromaior.
    """
    for resource in json_data.get("executionResources", []):
        if resource.get("path", "").endswith("Castromaior_06_10_2023"):
            for item in resource.get("data", []):
                if item.get("name") == "BBOX" and item.get("value"):
                    return item["value"]
    raise ValueError("No se encontró el BBOX en el recurso Castromaior.")



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
                INSERT INTO algoritmos.algorithm_metashape (
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
    tiff_files = [path for name, path in temp_files if name.lower().endswith(".tif")]
    wms_layers_info = []
    wms_server_tiff = None
    wms_layer_tiff = None
    wms_description_tiff = "Capa raster GeoTIFF publicada en GeoServer"

    
    for tif_file in tiff_files:
        with open(tif_file, 'rb') as f:
            file_data = f.read()
        
        layer_name = f"{GENERIC_LAYER}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
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

        # Actualizar capa genérica
        url_latest = f"{base_url}/workspaces/{WORKSPACE}/coveragestores/{GENERIC_LAYER}/file.geotiff"
        response_latest = requests.put(url_latest, headers=headers, data=file_data, auth=auth, params={"configure": "all"})
        if response_latest.status_code not in [201, 202]:
            raise Exception(f"Error actualizando capa genérica: {response_latest.text}")
        print(f"Capa genérica raster actualizada: {GENERIC_LAYER}")
        print(f"Raster disponible en: {base_url}/geoserver/{WORKSPACE}/wms?layers={WORKSPACE}:{layer_name}")

    return wms_layers_info



def generate_dynamic_xml(json_modificado, bbox, uuid_key, id_mission):

    fecha_completa = datetime.strptime(json_modificado['endTimestamp'], "%Y%m%dT%H%M%S")
    fecha = fecha_completa.date()

    titulo = "Metashape: " + datetime.now().strftime('%Y%m%d_%H%M%S')
    descripcion = "Descripción del metashape //TODO"

    #Mete ruta de minIO
    ruta_png: 0



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

              <gmd:graphicOverview>
                <gmd:MD_BrowseGraphic>
                    <gmd:fileName>
                    <gco:CharacterString>
                        {ruta_png}
                    </gco:CharacterString>
                    </gmd:fileName>
                </gmd:MD_BrowseGraphic>
                </gmd:graphicOverview>

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
                <gmd:resourceMaintenance/>

                <gmd:descriptiveKeywords>
                    <gmd:MD_Keywords>
                    <gmd:keyword>
                        <gco:CharacterString>WMS</gco:CharacterString>
                    </gmd:keyword>
                    <gmd:keyword>
                        <gco:CharacterString>WFS</gco:CharacterString>
                    </gmd:keyword>
                    <gmd:keyword>
                        <gco:CharacterString>Biodiversidad</gco:CharacterString>
                    </gmd:keyword>
                    <gmd:keyword>
                        <gco:CharacterString>Inspección acuática</gco:CharacterString>
                    </gmd:keyword>
                    <gmd:keyword>
                        <gco:CharacterString>USV</gco:CharacterString>
                    </gmd:keyword>
                    </gmd:MD_Keywords>
                </gmd:descriptiveKeywords>
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


                    <gmd:onLine>
                        <gmd:CI_OnlineResource>
                            <gmd:linkage>
                                <gmd:URL>{wms_server_tiff_escape}</gmd:URL>
                            </gmd:linkage>
                            <gmd:protocol>
                                <gco:CharacterString>OGC:WMS</gco:CharacterString>
                            </gmd:protocol>
                            <gmd:name>
                                <gco:CharacterString>{wms_layer_tiff}</gco:CharacterString>
                            </gmd:name>
                            <gmd:description>
                                <gco:CharacterString>{wms_description_tiff}</gco:CharacterString>
                            </gmd:description>
                            <gmd:function>
                                <gmd:CI_OnLineFunctionCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                        codeListValue="download"/>
                            </gmd:function>
                        </gmd:CI_OnlineResource>
                    </gmd:onLine>

                    <gmd:onLine>
                        <gmd:CI_OnlineResource>
                            <gmd:linkage>
                                <gmd:URL>{wms_server_shp_water}</gmd:URL>
                            </gmd:linkage>
                            <gmd:protocol>
                                <gco:CharacterString>OGC:WMS</gco:CharacterString>
                            </gmd:protocol>
                            <gmd:name>
                                <gco:CharacterString>{wms_layer_shp_water}</gco:CharacterString>
                            </gmd:name>
                            <gmd:description>
                                <gco:CharacterString>{wms_description_shp_water}</gco:CharacterString>
                            </gmd:description>
                            <gmd:function>
                                <gmd:CI_OnLineFunctionCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                        codeListValue="information"/>
                            </gmd:function>
                        </gmd:CI_OnlineResource>
                    </gmd:onLine>

                    <gmd:onLine>
                        <gmd:CI_OnlineResource>
                            <gmd:linkage>
                                <gmd:URL>{wfs_server_shp_water}</gmd:URL>
                            </gmd:linkage>
                            <gmd:protocol>
                                <gco:CharacterString>OGC:WFS-1.0.0-http-get-capabilities</gco:CharacterString>
                            </gmd:protocol>
                            <gmd:name>
                                <gco:CharacterString>{wms_layer_shp_water}</gco:CharacterString>
                            </gmd:name>
                            <gmd:description>
                                <gco:CharacterString>{wfs_description_shp_water}</gco:CharacterString>
                            </gmd:description>
                            <gmd:function>
                                <gmd:CI_OnLineFunctionCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                        codeListValue="download"/>
                            </gmd:function>
                        </gmd:CI_OnlineResource>
                    </gmd:onLine>


                    <gmd:onLine>
                        <gmd:CI_OnlineResource>
                            <gmd:linkage>
                                <gmd:URL>{ruta_csv}</gmd:URL>
                            </gmd:linkage>
                            <gmd:protocol>
                                <gco:CharacterString>WWW:DOWNLOAD-1.0-http--download</gco:CharacterString>
                            </gmd:protocol>
                            <gmd:name>
                                <gco:CharacterString>{csv_description}</gco:CharacterString>
                            </gmd:name>
                            <gmd:function>
                                <gmd:CI_OnLineFunctionCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                        codeListValue="download"/>
                            </gmd:function>
                        </gmd:CI_OnlineResource>
                    </gmd:onLine>


                    <gmd:onLine>
                        <gmd:CI_OnlineResource>
                            <gmd:linkage>
                                <gmd:URL>{ruta_tiff}</gmd:URL>
                            </gmd:linkage>
                            <gmd:protocol>
                                <gco:CharacterString>WWW:DOWNLOAD-1.0-http--download</gco:CharacterString>
                            </gmd:protocol>
                            <gmd:name>
                                <gco:CharacterString>{tif_description}</gco:CharacterString>
                            </gmd:name>
                            <gmd:function>
                                <gmd:CI_OnLineFunctionCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                        codeListValue="download"/>
                            </gmd:function>
                        </gmd:CI_OnlineResource>
                    </gmd:onLine>

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