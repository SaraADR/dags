import base64
import json
import os
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from flask import Config
import requests
import logging
import io  # Para manejar el archivo XML en memoria
from pyproj import Proj, transform, CRS
import re
from airflow.hooks.base import BaseHook
import boto3
from PIL import Image
import os
import os
import base64
import tempfile
import uuid
import json
import boto3
from botocore.config import Config
from airflow.hooks.base_hook import BaseHook
from PIL import Image




# Configurar el logging
logging.basicConfig(level=logging.INFO)

def convertir_coords(epsg_input,south, west, north, east):

    logging.info(f"Convirtiendo coordenadas de EPSG:{epsg_input} a EPSG:4326.")
    logging.info(f"Coordenadas antes de la conversión: sur={south}, oeste={west}, norte={north}, este={east}")
    

    # Crear objetos Proj para las proyecciones
    # Proyección de origen basada en la cadena EPSG "32629"
    crs_from = CRS.from_string(f"EPSG:{epsg_input}")
    proj_from = Proj(crs_from)

    # Proyección de destino, EPSG:4326 (WGS84, lat/long)
    crs_to = CRS.from_string("EPSG:4326")
    proj_to = Proj(crs_to)

    # Transformar de UTM a WGS84
    west2, south2 = transform(proj_from, proj_to, west, south)
    east2, north2 = transform(proj_from, proj_to, east, north)

    # Redondear las coordenadas a 6 decimales
    west2 = round(west2, 6)
    south2 = round(south2, 6)
    east2 = round(east2, 6)
    north2 = round(north2, 6)

    logging.info(f"Coordenadas después de la conversión: sur={south2}, oeste={west2}, norte={north2}, este={east2}")


    return south2, west2, north2, east2


def up_to_minio(temp_dir, filename):
    try:
        # Conexión a MinIO
        connection = BaseHook.get_connection('minio_conn')
        extra = json.loads(connection.extra)
        s3_client = boto3.client(
            's3',
            endpoint_url=extra['endpoint_url'],
            aws_access_key_id=extra['aws_access_key_id'],
            aws_secret_access_key=extra['aws_secret_access_key'],
            config=Config(signature_version='s3v4')
        )
        bucket_name = 'metashapetiffs'
        
        # Ruta completa del archivo local a subir
        local_file_path = os.path.join(temp_dir, filename)

        # Verificar que es un archivo
        if os.path.isfile(local_file_path):

            # Subir el archivo a MinIO
            s3_client.upload_file(local_file_path, bucket_name, f"{filename}")
            print(f"Archivo {filename} subido correctamente a MinIO.")
            
            # Generar la URL del archivo subido
            file_url = f"https://minioapi.avincis.cuatrodigital.com/{bucket_name}/{filename}"
            print(f"URL: {file_url}")
            return file_url

    except Exception as e:
        print(f"Error al subir archivos a MinIO: {str(e)}")
        return None


def tiff_to_jpg(tiff_path, jpg_path):
    try:
        # Abrir el archivo TIFF
        with Image.open(tiff_path) as img:
            # Convertir a modo RGB si no está en ese modo
            if img.mode != 'RGB':
                img = img.convert('RGB')
            
            # Guardar el archivo como JPG
            img.save(jpg_path, 'JPEG')
        
        print(f"Archivo convertido y guardado como {jpg_path}")

    except Exception as e:
        print(f"Error al convertir TIFF a JPG: {e}")

#Sube miniatura y el tiff #TODO TIFF A MINIO Y LLAMADA A GEOSERVER PARA IMPORTARLO 

def upload_miniature(**kwargs):
    files = kwargs['dag_run'].conf.get('otros', [])
    array_files = []

    with tempfile.TemporaryDirectory() as temp_dir:
        for file in files:
            file_name = file['file_name']

            # Si el archivo no termina en .tif o .tiff, continúa con el siguiente archivo
            if not (file_name.endswith('.tif') or file_name.endswith('.tiff')):
                continue

            # Decodificar el contenido del archivo desde base64
            file_content = base64.b64decode(file['content'])
            logging.info(file_name)

            # Crear la ruta completa del archivo dentro del directorio temporal
            temp_file_path = os.path.join(temp_dir, file_name)

            # Asegurarse de que la estructura del directorio existe
            os.makedirs(os.path.dirname(temp_file_path), exist_ok=True)

            # Guardar el contenido en el archivo temporal
            with open(temp_file_path, 'wb') as temp_file:
                temp_file.write(file_content)

            logging.info(f"Archivo guardado temporalmente en: {temp_file_path}")

            # Generar un UUID para el archivo convertido a JPG
            unique_key = str(uuid.uuid4())

            # Crear ruta para el archivo JPG
            file_jpg_name = f"{unique_key}.jpg"
            temp_jpg_path = os.path.join(temp_dir, file_jpg_name)

            # Convertir TIFF a JPG
            tiff_to_jpg(temp_file_path, temp_jpg_path)

            # Subir el archivo JPG a MinIO y obtener la URL
            file_url = up_to_minio(temp_dir, file_jpg_name)

            # Añadir nombre y URL al array de archivos
            array_files.append({'name': os.path.basename(file_name), 'url': file_url})

    return array_files



# Función para generar el XML
def generate_xml(**kwargs):
    logging.info("Iniciando la generación del XML.")

    xml_encoded = []
    
    algoritm_result = kwargs['dag_run'].conf.get('json')

    file_url_array = kwargs['ti'].xcom_pull(task_ids='upload_miniature')

    logging.info(f"Contenido JSON cargado: {algoritm_result}")

    executionResources = algoritm_result['executionResources']
    logging.info(f"Execution Resources encontrados: {len(executionResources)} recursos")


    # Se extrae la información del BBOX y el sistema de referencia
    outputFalse = next((obj for obj in executionResources if obj['output'] == False), None)['data']
    bboxData = next((obj for obj in outputFalse if obj['name'] == 'BBOX'), None)
    bbox = bboxData['value']
    coordinate_system = bboxData['ReferenceSystem']
    logging.info(f"Coordenadas del BBOX: {bbox} en sistema de referencia {coordinate_system}")


    # DATOS QUE NO VARIAN (SIEMPRE SON LOS MISMOS)

    organization_name = 'Avincis'
    email_address = ' admin@einforex.es'
    protocol = 'OGC:WMS-1.3.0-http-get-map'
    wms_link_conn =  BaseHook.get_connection('geoserver_capabilites')
    wms_link = wms_link_conn.host
            
     
    # Coords BBOX
    west_bound_pre = bbox['westBoundLongitude']
    east_bound_pre = bbox['eastBoundLongitude']
    south_bound_pre = bbox['southBoundLatitude']
    north_bound_pre = bbox['northBoundLatitude']

    logging.info("Llamando a convertir_coords.")

    # Función de conversión (debe estar definida en tu código)
    west_bound,south_bound,east_bound,north_bound= convertir_coords (coordinate_system, south_bound_pre,west_bound_pre,north_bound_pre, east_bound_pre)

    # Procesar recursos de salida
    for resource in executionResources:
        if resource['output'] == False:
            logging.info("Saltando recurso que no es de salida.")
            continue
            

        if not re.search(r'\.tif$', resource['path'], re.IGNORECASE):
            logging.info("Saltando recurso que no es un archivo TIFF.")
            continue

        identifier = next((obj for obj in resource['data'] if obj['name'] == 'identifier'), None)["value"]
        spatial_resolution = next((obj for obj in resource['data'] if obj['name'] == 'pixelSize'), None)["value"]
        specificUsage = next((obj for obj in resource['data'] if obj['name'] == 'specificUsage'), None)["value"]

        file_name = os.path.basename(resource['path'])
        miniature_url = next((item['url'] for item in file_url_array if item['name'] == file_name), None)
        
        logging.info(file_url_array)

        logging.info(miniature_url)

        logging.info (file_name)

        logging.info(f"Procesando recurso con identifier={identifier} y resolución={spatial_resolution}")

        # Ensure spatial_resolution (float) is converted to a string
        spatial_resolution_str = str(spatial_resolution)

        # Datos para el XML
        layer_name = identifier
        title = identifier       

      # JSON dinámico con los valores correspondientes
        wms_link = algoritm_result['executionResources'][0]['path']  # Link de WMS para este recurso específico
        layer_description = "Descripción de la capa generada"  # Puedes extraer o generar esto según el contexto
        file_identifier = identifier # Un identificador único (se puede derivar)
        date_stamp = datetime.now().isoformat()
        publication_date = "2024-07-29"  # Basado en la fecha proporcionada en el archivo

        logging.info("Llamando a la función creador_xml_metadata.")
        
        # Generate XML tree
        tree = creador_xml_metadata(
            wmsLayer=layer_name,
            file_identifier=file_identifier,
            organization_name=organization_name,
            email_address=email_address,
            date_stamp=date_stamp,
            title=title,
            publication_date=publication_date,
            west_bound=west_bound,
            east_bound=east_bound,
            south_bound=south_bound,
            north_bound=north_bound,
            spatial_resolution=spatial_resolution,
            specificUsage = specificUsage,
            protocol=protocol,
            wms_link=wms_link,
            layer_name=layer_name,
            miniature_url=miniature_url,
            layer_description=layer_description
        )

        if tree is None:
            logging.error("La función creador_xml_metadata retornó None. Asegúrate de que está retornando un ElementTree válido.")
            raise Exception("Error: creador_xml_metadata retornó None.")

        logging.info("El XML ha sido creado exitosamente en memoria.")

        # Convert the XML tree to bytes
        xml_bytes_io = io.BytesIO()
        tree.write(xml_bytes_io, encoding='utf-8', xml_declaration=True)
        xml_content = xml_bytes_io.getvalue()

        # Base64 encode the XML bytes
        xml_encoded_to_push = base64.b64encode(xml_content).decode('utf-8')

        xml_encoded.append (xml_encoded_to_push)


        # logging.info (f"Xml enconded {xml_encoded}")

    # Store the base64 encoded XML content in XCom
    return xml_encoded




# Función para obtener las credenciales de GeoNetwork
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



# Función para subir el XML utilizando las credenciales obtenidas
def upload_to_geonetwork(**context):
    try:
        # Obtener los tokens de autenticación
        access_token, xsrf_token, set_cookie_header = get_geonetwork_credentials()

        # Obtener el XML base64 desde XCom
        xml_data_array = context['ti'].xcom_pull(task_ids='generate_xml')

        for xml_data in xml_data_array:
        
            xml_decoded = base64.b64decode(xml_data).decode('utf-8')

            # # Convertir el contenido XML a un objeto de tipo stream (equivalente a createReadStream en Node.js)
            # xml_file_stream = io.StringIO(xml_decoded)

            logging.info(f"XML DATA: {xml_data}")
            logging.info(xml_decoded)

            data = {
                'metadataType': (None, 'METADATA'),
                'uuidProcessing': (None, 'NOTHING'),
                'transformWith': (None, '_none_'),
                'group': (None, 2),
                'category': (None, ''),
                'file': ('nombre_archivo.xml', xml_decoded, 'text/xml'),
            }
            
            files = {
                'file': ('nombre_archivo.xml', xml_decoded, 'text/xml'),
            }

            # URL de GeoNetwork para subir el archivo XML
            upload_url = "https://eiiob.dev.cuatrodigital.com/geonetwork/srv/api/records"

            # Encabezados que incluyen los tokens
            headers = {
                # 'Content-Type': 'multipart/form-data',
                'Authorization': f"Bearer {access_token}",
                'x-xsrf-token': str(xsrf_token),
                'Cookie': str(set_cookie_header[0]),
                'Accept': 'application/json'
            }

            # Realizar la solicitud POST para subir el archivo XML
            logging.info(f"Subiendo XML a la URL: {upload_url}")
            response = requests.post(upload_url,files=files,data=data, headers=headers)
            logging.info(response)

            # Verificar si hubo algún error en la solicitud
            response.raise_for_status()

            logging.info(f"Archivo subido correctamente a GeoNetwork. Respuesta: {response.text}")
    except Exception as e:
        if response is not None:
            logging.error(f"Código de estado: {response.status_code}, Respuesta: {response.text}")

        logging.error(f"Error al subir el archivo a GeoNetwork: {e}")
        raise Exception(f"Error al subir el archivo a GeoNetwork: {e}")


# Función para crear el XML metadata
def creador_xml_metadata(file_identifier, specificUsage, wmsLayer, miniature_url, organization_name, email_address, date_stamp, title, publication_date, west_bound, east_bound, south_bound, north_bound, spatial_resolution, protocol, wms_link, layer_name, layer_description):
    logging.info("Iniciando la creación del XML.")

    root = ET.Element("gmd:MD_Metadata", {
        "xmlns:gmd": "http://www.isotc211.org/2005/gmd",
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xmlns:gco": "http://www.isotc211.org/2005/gco",
        "xmlns:srv": "http://www.isotc211.org/2005/srv",
        "xmlns:gmx": "http://www.isotc211.org/2005/gmx",
        "xmlns:gts": "http://www.isotc211.org/2005/gts",
        "xmlns:gsr": "http://www.isotc211.org/2005/gsr",
        "xmlns:gmi": "http://www.isotc211.org/2005/gmi",
        "xmlns:gml": "http://www.opengis.net/gml/3.2",
        "xmlns:xlink": "http://www.w3.org/1999/xlink",
        "xsi:schemaLocation": "http://www.isotc211.org/2005/gmd http://schemas.opengis.net/csw/2.0.2/profiles/apiso/1.0.0/apiso.xsd"
    })

 # fileIdentifier
    fid = ET.SubElement(root, "gmd:fileIdentifier")
    fid_cs = ET.SubElement(fid, "gco:CharacterString")
    fid_cs.text = str(file_identifier)

    
        # Padre gmd:descriptiveKeywords
    gmd_descriptiveKeywords = ET.SubElement(root, "gmd:descriptiveKeywords")

    # Añadir gmd:resourceConstraints dentro de descriptiveKeywords
    gmd_resourceConstraints = ET.SubElement(gmd_descriptiveKeywords, "gmd:resourceConstraints")

    # Añadir gmd:MD_LegalConstraints
    gmd_MD_LegalConstraints = ET.SubElement(gmd_resourceConstraints, "gmd:MD_LegalConstraints")

    # Añadir gmd:accessConstraints dentro de MD_LegalConstraints
    gmd_accessConstraints = ET.SubElement(gmd_MD_LegalConstraints, "gmd:accessConstraints")

    # Añadir gmd:MD_RestrictionCode dentro de accessConstraints
    gmd_MD_RestrictionCode = ET.SubElement(gmd_accessConstraints, "gmd:MD_RestrictionCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_RestrictionCode",
        "codeListValue": "otherRestrictions"
    })
    gmd_MD_RestrictionCode.text = "z"

    # Añadir gmd:otherConstraints dentro de MD_LegalConstraints
    gmd_otherConstraints = ET.SubElement(gmd_MD_LegalConstraints, "gmd:otherConstraints")

    # Añadir gmx:Anchor dentro de otherConstraints
    gmx_Anchor = ET.SubElement(gmd_otherConstraints, "gmx:Anchor", {
        "xlink:href": "http://inspire.ec.europa.eu/metadata-codelist/LimitationsOnPublicAccess/noLimitations"
    })
    gmx_Anchor.text = "Sin limitaciones al acceso público"

    # WMS LAYER 
   
    gmd_distributionInfo = ET.SubElement(root, "gmd:distributionInfo")
    gmd_MD_Distribution = ET.SubElement(gmd_distributionInfo, "gmd:MD_Distribution")
    gmd_transferOptions = ET.SubElement(gmd_MD_Distribution, "gmd:transferOptions")

    gmd_MD_DigitalTransferOptions = ET.SubElement(gmd_transferOptions, "gmd:MD_DigitalTransferOptions")
    gmd_onLine = ET.SubElement(gmd_MD_DigitalTransferOptions, "gmd:onLine")
    gmd_CI_OnlineResource = ET.SubElement(gmd_onLine, "gmd:CI_OnlineResource")
    linkage = ET.SubElement(gmd_CI_OnlineResource, "gmd:linkage")
    linkageUrl = ET.SubElement(linkage, "gmd:URL")
    linkageUrl.text = "https://geoserver.dev.cuatrodigital.com/geoserver/tests-geonetwork/wms"

    protocol = ET.SubElement(gmd_CI_OnlineResource, "gmd:protocol")
    protocolCharacterString = ET.SubElement(protocol, "gco:CharacterString")
    protocolCharacterString.text = "OGC:WMS-1.3.0-http-get-map"

    name = ET.SubElement(gmd_CI_OnlineResource, "gmd:name")
    nameCharacterString = ET.SubElement(name, "gco:CharacterString")
    nameCharacterString.text = wmsLayer

    description = ET.SubElement(gmd_CI_OnlineResource, "gmd:description")
    descriptionCharacterString = ET.SubElement(description, "gco:CharacterString")
    descriptionCharacterString.text = "Capa 0026 de prueba"
    
    function = ET.SubElement(gmd_CI_OnlineResource, "gmd:function")
    functionCharacterString = ET.SubElement(function, "gco:CI_OnLineFunctionCode")
    functionCharacterString = ET.SubElement(functionCharacterString, "gmd:CI_OnLineFunctionCode", {
        "codeList":"http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode",
        "codeListValue": "download"
    })


    # Añadir characterSet
    characterSet = ET.SubElement(root, "gmd:characterSet")
    char_set_code = ET.SubElement(characterSet, "gmd:MD_CharacterSetCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_CharacterSetCode",
        "codeListValue": "utf8"
    })

    # Añadir parentIdentifier
    parentIdentifier = ET.SubElement(root, "gmd:parentIdentifier", {"gco:nilReason": "missing"})

    # Añadir hierarchyLevel
    hierarchyLevel = ET.SubElement(root, "gmd:hierarchyLevel")
    scope_code = ET.SubElement(hierarchyLevel, "gmd:MD_ScopeCode", {
        "codeList": "./resources/codelist.xml#MD_ScopeCode",
        "codeListValue": "dataset"
    })
    scope_code.text = "dataset"

    # Añadir contact
    contact = ET.SubElement(root, "gmd:contact")
    responsibleParty = ET.SubElement(contact, "gmd:CI_ResponsibleParty")
    orgName = ET.SubElement(responsibleParty, "gmd:organisationName")
    gco_characterString = ET.SubElement(orgName, "gco:CharacterString")
    gco_characterString.text = "Avincis Technics"

    # Añadir contactInfo
    contactInfo = ET.SubElement(responsibleParty, "gmd:contactInfo")
    ciContact = ET.SubElement(contactInfo, "gmd:CI_Contact")
    address = ET.SubElement(ciContact, "gmd:address")
    ciAddress = ET.SubElement(address, "gmd:CI_Address")
    email = ET.SubElement(ciAddress, "gmd:electronicMailAddress")
    gco_characterString = ET.SubElement(email, "gco:CharacterString")
    gco_characterString.text = "admin@einforex.es"

    # Añadir role
    role = ET.SubElement(responsibleParty, "gmd:role")
    role_code = ET.SubElement(role, "gmd:CI_RoleCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_RoleCode",
        "codeListValue": "pointOfContact"
    })

    # Añadir dateStamp
    dateStamp = ET.SubElement(root, "gmd:dateStamp")
    date_time = ET.SubElement(dateStamp, "gco:DateTime")
    date_time.text = "2024-07-12T05:22:04"

    # Añadir metadataStandardName
    metadataStandardName = ET.SubElement(root, "gmd:metadataStandardName")
    gco_characterString = ET.SubElement(metadataStandardName, "gco:CharacterString")
    gco_characterString.text = "NEM: ISO 19115:2003 + Reglamento (CE) Nº 1205/2008 de Inspire"

    # Añadir metadataStandardVersion
    metadataStandardVersion = ET.SubElement(root, "gmd:metadataStandardVersion")
    gco_characterString = ET.SubElement(metadataStandardVersion, "gco:CharacterString")
    gco_characterString.text = "1.2"

    # Añadir referenceSystemInfo
    ref_sys_info = ET.SubElement(root, "gmd:referenceSystemInfo")
    md_ref_sys = ET.SubElement(ref_sys_info, "gmd:MD_ReferenceSystem")
    ref_sys_id = ET.SubElement(md_ref_sys, "gmd:referenceSystemIdentifier")
    rs_id = ET.SubElement(ref_sys_id, "gmd:RS_Identifier")
    code = ET.SubElement(rs_id, "gmd:code")
    gco_characterString = ET.SubElement(code, "gco:CharacterString")
    gco_characterString.text = "EPSG:32629"
    code_space = ET.SubElement(rs_id, "gmd:codeSpace")
    gco_characterString = ET.SubElement(code_space, "gco:CharacterString")
    gco_characterString.text = "http://www.ign.es"

    # Añadir identificationInfo
    identificationInfo = ET.SubElement(root, "gmd:identificationInfo")
    md_data_identification = ET.SubElement(identificationInfo, "gmd:MD_DataIdentification")

    # Añadir citation
    citation = ET.SubElement(md_data_identification, "gmd:citation")
    ci_citation = ET.SubElement(citation, "gmd:CI_Citation")
    title = ET.SubElement(ci_citation, "gmd:title")
    gco_characterString = ET.SubElement(title, "gco:CharacterString")
    gco_characterString.text = file_identifier

    # Añadir publication date
    pub_date = ET.SubElement(ci_citation, "gmd:date")
    ci_date = ET.SubElement(pub_date, "gmd:CI_Date")
    date = ET.SubElement(ci_date, "gmd:date")
    gco_date = ET.SubElement(date, "gco:Date")
    gco_date.text = "2024-07-29"
    date_type = ET.SubElement(ci_date, "gmd:dateType")
    ci_date_type = ET.SubElement(date_type, "gmd:CI_DateTypeCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_DateTypeCode",
        "codeListValue": "publication"
    })

    # Añadir abstract
    abstract = ET.SubElement(md_data_identification, "gmd:abstract")
    gco_characterString = ET.SubElement(abstract, "gco:CharacterString")
    gco_characterString.text = "Conocimiento y estudio con precisión de la ubicación, forma y dimensiones en el espacio de los objetos presentes en la superficie de la tierra"

    # Añadir resourceMaintenance
    resourceMaintenance = ET.SubElement(md_data_identification, "gmd:resourceMaintenance")
    md_maintenance_information = ET.SubElement(resourceMaintenance, "gmd:MD_MaintenanceInformation")
    maintenance_frequency = ET.SubElement(md_maintenance_information, "gmd:maintenanceAndUpdateFrequency")
    md_frequency_code = ET.SubElement(maintenance_frequency, "gmd:MD_FrequencyCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_MaintenanceFrequencyCode",
        "codeListValue": "notPlanned"
    })

    # Añadir graphicOverview
    graphicOverview = ET.SubElement(md_data_identification, "gmd:graphicOverview")
    md_browse_graphic = ET.SubElement(graphicOverview, "gmd:MD_BrowseGraphic")
    fileName = ET.SubElement(md_browse_graphic, "gmd:fileName")
    gco_characterString = ET.SubElement(fileName, "gco:CharacterString")
    gco_characterString.text = miniature_url
    fileDescription = ET.SubElement(md_browse_graphic, "gmd:fileDescription")
    gco_characterString = ET.SubElement(fileDescription, "gco:CharacterString")
    fileType = ET.SubElement(md_browse_graphic, "gmd:fileType")
    gco_characterString = ET.SubElement(fileType, "gco:CharacterString")
    

    # Añadir descriptiveKeywords (primero)
    descriptiveKeywords = ET.SubElement(md_data_identification, "gmd:descriptiveKeywords")
    descriptiveKeywords.text = "Ortomosáico"
    md_keywords = ET.SubElement(descriptiveKeywords, "gmd:MD_Keywords")
    keywords = ["photogrametry", "burst", "orthomosaic", "RGB", "aerial-photography", "Opendata"]

    for keyword in keywords:
        gmd_keyword = ET.SubElement(md_keywords, "gmd:keyword")
        gco_CharacterString = ET.SubElement(gmd_keyword, "gco:CharacterString")
        gco_CharacterString.text = keyword

    # Añadir type y thesaurusName
    gmd_type = ET.SubElement(md_keywords, "gmd:type")
    gmd_MD_KeywordTypeCode = ET.SubElement(gmd_type, "gmd:MD_KeywordTypeCode", {
        "codeList": "https://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_KeywordTypeCode",
        "codeListValue": "theme"
    })

    thesaurusName = ET.SubElement(md_keywords, "gmd:thesaurusName")
    ci_citation = ET.SubElement(thesaurusName, "gmd:CI_Citation")
    gmd_title = ET.SubElement(ci_citation, "gmd:title")
    gmx_anchor = ET.SubElement(gmd_title, "gmx:Anchor", {
        "xlink:href": "http://www.eionet.europa.eu/gemet/inspire_themes"
    })
    gmx_anchor.text = "Keywords"

    # Añadir date
    gmd_date = ET.SubElement(ci_citation, "gmd:date")
    ci_date = ET.SubElement(gmd_date, "gmd:CI_Date")
    gmd_date_inner = ET.SubElement(ci_date, "gmd:date")
    gco_Date = ET.SubElement(gmd_date_inner, "gco:Date")
    gco_Date.text = "2008-06-01"

    # Añadir dateType
    date_type = ET.SubElement(ci_date, "gmd:dateType")
    gmd_CI_DateTypeCode = ET.SubElement(date_type, "gmd:CI_DateTypeCode", {
        "codeList": "https://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_DateTypeCode",
        "codeListValue": "publication"
    })

    # Añadir la segunda sección de descriptiveKeywords
    descriptiveKeywords2 = ET.SubElement(md_data_identification, "gmd:descriptiveKeywords")
    md_keywords2 = ET.SubElement(descriptiveKeywords2, "gmd:MD_Keywords")
    keyword_anchor = ET.SubElement(md_keywords2, "gmd:keyword")
    gmx_anchor2 = ET.SubElement(keyword_anchor, "gmx:Anchor", {
        "xlink:href": "http://inspire.ec.europa.eu/theme/oi"
    })
    gmx_anchor2.text = "Ortoimágenes"

    # Añadir type
    gmd_type2 = ET.SubElement(md_keywords2, "gmd:type")
    gmd_MD_KeywordTypeCode2 = ET.SubElement(gmd_type2, "gmd:MD_KeywordTypeCode", {
        "codeList": "https://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_KeywordTypeCode",
        "codeListValue": "theme"
    })

    # Añadir thesaurusName
    thesaurusName2 = ET.SubElement(md_keywords2, "gmd:thesaurusName")
    ci_citation2 = ET.SubElement(thesaurusName2, "gmd:CI_Citation")
    gmd_title2 = ET.SubElement(ci_citation2, "gmd:title")
    gmx_anchor3 = ET.SubElement(gmd_title2, "gmx:Anchor", {
        "xlink:href": "http://www.eionet.europa.eu/gemet/inspire_themes"
    })
    gmx_anchor3.text = "Temas INSPIRE"

    # Añadir date
    gmd_date2 = ET.SubElement(ci_citation2, "gmd:date")
    ci_date2 = ET.SubElement(gmd_date2, "gmd:CI_Date")
    gmd_date_inner2 = ET.SubElement(ci_date2, "gmd:date")
    gco_Date2 = ET.SubElement(gmd_date_inner2, "gco:Date")
    gco_Date2.text = "2008-06-01"

    # Añadir dateType
    date_type2 = ET.SubElement(ci_date2, "gmd:dateType")
    gmd_CI_DateTypeCode3 = ET.SubElement(date_type2, "gmd:CI_DateTypeCode", {
        "codeList": "https://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_DateTypeCode",
        "codeListValue": "publication"
    })

    # Añadir resourceSpecificUsage
    resourceSpecificUsage = ET.SubElement(md_data_identification, "gmd:resourceSpecificUsage")
    md_usage = ET.SubElement(resourceSpecificUsage, "gmd:MD_Usage")
    specificUsage = ET.SubElement(md_usage, "gmd:specificUsage")
    gco_characterString = ET.SubElement(specificUsage, "gco:CharacterString")
    gco_characterString.text = "Uso para análisis geoespacial detallado en estudios ambientales y urbanísticos"

    # Añadir userContactInfo
    userContactInfo = ET.SubElement(md_usage, "gmd:userContactInfo")
    ci_responsible_party = ET.SubElement(userContactInfo, "gmd:CI_ResponsibleParty")
    individualName = ET.SubElement(ci_responsible_party, "gmd:individualName", {"gco:nilReason": "inapplicable"})
    organisationName = ET.SubElement(ci_responsible_party, "gmd:organisationName")
    gco_characterString = ET.SubElement(organisationName, "gco:CharacterString")
    gco_characterString.text = "Avincis"
    contactInfo = ET.SubElement(ci_responsible_party, "gmd:contactInfo")
    ci_contact = ET.SubElement(contactInfo, "gmd:CI_Contact")
    address = ET.SubElement(ci_contact, "gmd:address")
    ci_address = ET.SubElement(address, "gmd:CI_Address")
    email = ET.SubElement(ci_address, "gmd:electronicMailAddress")
    gco_characterString = ET.SubElement(email, "gco:CharacterString")
    gco_characterString.text = " admin@einforex.es"
    role = ET.SubElement(ci_responsible_party, "gmd:role")
    role_code = ET.SubElement(role, "gmd:CI_RoleCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_RoleCode",
        "codeListValue": "user"
    })

    # Añadir resourceConstraints (primera parte)
    resourceConstraints = ET.SubElement(md_data_identification, "gmd:resourceConstraints")
    md_legal_constraints = ET.SubElement(resourceConstraints, "gmd:MD_LegalConstraints")
    accessConstraints = ET.SubElement(md_legal_constraints, "gmd:accessConstraints")
    md_restriction_code = ET.SubElement(accessConstraints, "gmd:MD_RestrictionCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_RestrictionCode",
        "codeListValue": "otherRestrictions"
    })
    otherConstraints = ET.SubElement(md_legal_constraints, "gmd:otherConstraints")
    gmx_anchor = ET.SubElement(otherConstraints, "gmx:Anchor", {
        "xlink:href": "http://inspire.ec.europa.eu/metadata-codelist/LimitationsOnPublicAccess/noLimitations"
    })
    gmx_anchor.text = "Sin limitaciones al acceso público"

    # Añadir resourceConstraints (otras partes con nilReason)
    resourceConstraints_nil1 = ET.SubElement(md_data_identification, "gmd:resourceConstraints", {"gco:nilReason": "missing"})
    resourceConstraints_nil2 = ET.SubElement(md_data_identification, "gmd:resourceConstraints", {"gco:nilReason": "missing"})

    # Añadir spatialRepresentationType
    spatial_rep_type = ET.SubElement(md_data_identification, "gmd:spatialRepresentationType")
    md_spatial_rep_type_code = ET.SubElement(spatial_rep_type, "gmd:MD_SpatialRepresentationTypeCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_SpatialRepresentationTypeCode",
        "codeListValue": "grid"
    })

    # Añadir spatialResolution
    spatial_res = ET.SubElement(md_data_identification, "gmd:spatialResolution")
    md_resolution = ET.SubElement(spatial_res, "gmd:MD_Resolution")
    distance = ET.SubElement(md_resolution, "gmd:distance")
    gco_distance = ET.SubElement(distance, "gco:Distance", {"uom": "metros"})
    gco_distance.text = "0.026"
    

    # Añadir categories (categoría) dentro de MD_DataIdentification
    topicCategory = ET.SubElement(md_data_identification, "gmd:topicCategory")
    topicCategoryCode = ET.SubElement(topicCategory, "gmd:MD_TopicCategoryCode")
    topicCategoryCode.text = "imageryBaseMapsEarthCover"


    # Añadir language (idioma) después de las categorías en MD_DataIdentification
    language = ET.SubElement(md_data_identification, "gmd:language")
    lang_code = ET.SubElement(language, "gmd:LanguageCode", {
        "codeList": "http://www.loc.gov/standards/iso639-2/",
        "codeListValue": "spa"
    })
    lang_code.text = "Spanish"


    # Añadir extent (bounding box)
    extent = ET.SubElement(md_data_identification, "gmd:extent")
    ex_extent = ET.SubElement(extent, "gmd:EX_Extent")
    geographic_element = ET.SubElement(ex_extent, "gmd:geographicElement")
    bbox = ET.SubElement(geographic_element, "gmd:EX_GeographicBoundingBox")
    west_bound = ET.SubElement(bbox, "gmd:westBoundLongitude")
    gco_decimal = ET.SubElement(west_bound, "gco:Decimal")
    gco_decimal.text = "-7.6392"
    east_bound = ET.SubElement(bbox, "gmd:eastBoundLongitude")
    gco_decimal = ET.SubElement(east_bound, "gco:Decimal")
    gco_decimal.text = "-7.6336"
    south_bound = ET.SubElement(bbox, "gmd:southBoundLatitude")
    gco_decimal = ET.SubElement(south_bound, "gco:Decimal")
    gco_decimal.text = "42.8025"
    north_bound = ET.SubElement(bbox, "gmd:northBoundLatitude")
    gco_decimal = ET.SubElement(north_bound, "gco:Decimal")
    gco_decimal.text = "42.8044"

    # Añadir presentationForm
    presentation_form = ET.SubElement(md_data_identification, "gmd:presentationForm")
    gco_characterString = ET.SubElement(presentation_form, "gco:CharacterString")
    gco_characterString.text = "Modelo Digital"

    # Añadir distributionInfo
    distribution_info = ET.SubElement(root, "gmd:distributionInfo")
    md_distribution = ET.SubElement(distribution_info, "gmd:MD_Distribution")
    transfer_options = ET.SubElement(md_distribution, "gmd:transferOptions")
    digital_transfer = ET.SubElement(transfer_options, "gmd:MD_DigitalTransferOptions")
    on_line = ET.SubElement(digital_transfer, "gmd:onLine")
    ci_online_resource = ET.SubElement(on_line, "gmd:CI_OnlineResource")
    protocol = ET.SubElement(ci_online_resource, "gmd:protocol")
    gco_characterString = ET.SubElement(protocol, "gco:CharacterString")
    gco_characterString.text = "OGC:WMS-1.3.0-http-get-map"
    name = ET.SubElement(ci_online_resource, "gmd:name")
    gco_characterString = ET.SubElement(name, "gco:CharacterString")
    gco_characterString.text = "a__0026_4740004_611271"
    description = ET.SubElement(ci_online_resource, "gmd:description")
    gco_characterString = ET.SubElement(description, "gco:CharacterString")
    gco_characterString.text = "Capa 0026 de prueba"
    function = ET.SubElement(ci_online_resource, "gmd:function")
    ci_online_function_code = ET.SubElement(function, "gmd:CI_OnLineFunctionCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode",
        "codeListValue": "download"
    })

    # Añadir resourceMaintenance (actualización dinámica)
    resource_maintenance = ET.SubElement(root, "gmd:resourceMaintenance")
    md_maintenance_information = ET.SubElement(resource_maintenance, "gmd:MD_MaintenanceInformation")
    maintenance_frequency = ET.SubElement(md_maintenance_information, "gmd:maintenanceAndUpdateFrequency")
    md_frequency_code = ET.SubElement(maintenance_frequency, "gmd:MD_FrequencyCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_MaintenanceFrequencyCode",
        "codeListValue": "continual"
    })
    maintenance_note = ET.SubElement(md_maintenance_information, "gmd:maintenanceNote")
    gco_characterString = ET.SubElement(maintenance_note, "gco:CharacterString")
    gco_characterString.text = "El contenido del dataset puede actualizarse de manera dinámica conforme se adquieran nuevos datos."

    # Añadir Licencias específicas
    resource_constraints = ET.SubElement(root, "gmd:resourceConstraints")
    md_legal_constraints = ET.SubElement(resource_constraints, "gmd:MD_LegalConstraints")
    access_constraints = ET.SubElement(md_legal_constraints, "gmd:accessConstraints")
    md_restriction_code = ET.SubElement(access_constraints, "gmd:MD_RestrictionCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_RestrictionCode",
        "codeListValue": "license"
    })
    other_constraints = ET.SubElement(md_legal_constraints, "gmd:otherConstraints")
    gco_characterString = ET.SubElement(other_constraints, "gco:CharacterString")
    gco_characterString.text = "Licencia específica por perfil, según lo indicado por el proveedor de datos."

    # Añadir DQ_DataQuality
    dq_data_quality = ET.SubElement(root, "gmd:DQ_DataQuality")
    dq_scope = ET.SubElement(dq_data_quality, "gmd:scope")
    dq_scope_inner = ET.SubElement(dq_scope, "gmd:DQ_Scope")
    level = ET.SubElement(dq_scope_inner, "gmd:level")
    md_scope_code = ET.SubElement(level, "gmd:MD_ScopeCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_ScopeCode",
        "codeListValue": "dataset"
    })
    md_scope_code.text = "dataset"

    # Añadir DQ report
    dq_report = ET.SubElement(dq_data_quality, "gmd:report")
    dq_domain_consistency = ET.SubElement(dq_report, "gmd:DQ_DomainConsistency")
    result = ET.SubElement(dq_domain_consistency, "gmd:result")
    dq_conformance_result = ET.SubElement(result, "gmd:DQ_ConformanceResult")
    specification = ET.SubElement(dq_conformance_result, "gmd:specification")
    ci_citation = ET.SubElement(specification, "gmd:CI_Citation")
    title = ET.SubElement(ci_citation, "gmd:title")
    gmx_anchor = ET.SubElement(title, "gmx:Anchor", {
        "xlink:href": "https://data.europa.eu/eli/reg/2010/1089"
    })
    gmx_anchor.text = ("Reglamento (UE) n o 1089/2010 de la Comisión de 23 de noviembre de 2010 por "
                       "el que se aplica la Directiva 2007/2/CE del Parlamento Europeo y del Consejo "
                       "en lo que se refiere a la interoperabilidad de los conjuntos y los servicios de datos espaciales")

    # Añadir lineage
    lineage = ET.SubElement(dq_data_quality, "gmd:lineage")
    li_lineage = ET.SubElement(lineage, "gmd:LI_Lineage")
    statement = ET.SubElement(li_lineage, "gmd:statement")
    gco_characterString = ET.SubElement(statement, "gco:CharacterString")
    gco_characterString.text = ("Ortomosaico generado mediante el procesamiento de imágenes aéreas capturadas con una "
                                "cobertura longitudinal del 60% y transversal del 30%, con un error medio cuadrático de hasta 40 cm.")

    return ET.ElementTree(root)


# Definición del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'retries': 1,
}

dag = DAG(
    'metashape_rgb',
    default_args=default_args,
    description='DAG para generar metadatos XML y subirlos a GeoNetwork',
    schedule_interval=None,  # Se puede ajustar según necesidades
    catchup=False
)

# Tarea 1: Generar el XML
generate_xml_task = PythonOperator(
    task_id='generate_xml',
    python_callable=generate_xml,
    provide_context=True,
    dag=dag
)

# Tarea 2: Subir miniatura
upload_miniature_task = PythonOperator(
    task_id='upload_miniature',
    python_callable=upload_miniature,
    provide_context=True,
    dag=dag
)

# Tarea 3: Subir el XML a GeoNetwork
upload_xml_task = PythonOperator(
    task_id='upload_to_geonetwork',
    python_callable=upload_to_geonetwork,
    provide_context=True,
    dag=dag
)

# Definir el flujo de las tareas
upload_miniature_task >> generate_xml_task>> upload_xml_task