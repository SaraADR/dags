import base64
import os
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import logging
import io  # Para manejar el archivo XML en memoria
from pyproj import Proj, transform, CRS
import re

# Configurar la URL de GeoNetwork
geonetwork_url = "https://eiiob.dev.cuatrodigital.com/geonetwork/srv/api"

# Configurar el logging
logging.basicConfig(level=logging.INFO)

def convertir_coords(epsg_input,south, west, north, east):


    # Entrada: EPSG de la proyección origen, en formato cadena (e.g., "32629")
    # epsg_input = "32629"  # UTM Zona 29 Norte


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

    return south2, west2, north2, east2

    

# Función para generar el XML
def generate_xml(**kwargs):
    logging.info("Iniciando la generación del XML.")

    xml_encoded = []
    
    algoritm_result = kwargs['dag_run'].conf.get('json')

    logging.info(f"Contenido JSON cargado: {algoritm_result}")

    executionResources = algoritm_result['executionResources']

    # Se extrae la información del BBOX y el sistema de referencia
    outputFalse = next((obj for obj in executionResources if obj['output'] == False), None)['data']
    bboxData = next((obj for obj in outputFalse if obj['name'] == 'BBOX'), None)
    bbox = bboxData['value']
    coordinate_system = bboxData['ReferenceSystem']


    
    # DATOS QUE NO VARIAN (SIEMPRE SON LOS MISMOS)

    organization_name = 'Avincis'
    email_address = 'avincis@organizacion.es'
    protocol = 'OGC:WMS-1.3.0-http-get-map'
    wms_link = 'https://geoserver.dev.cuatrodigital.com/geoserver/tests-geonetwork/wms'

    # Coords BBOX
    west_bound_pre = bbox['westBoundLongitude']
    east_bound_pre = bbox['eastBoundLongitude']
    south_bound_pre = bbox['southBoundLatitude']
    north_bound_pre = bbox['northBoundLatitude']

    # Función de conversión (debe estar definida en tu código)
    south_bound, west_bound, north_bound, east_bound = convertir_coords (coordinate_system, south_bound_pre,west_bound_pre,north_bound_pre, east_bound_pre)

    # Procesar recursos de salida
    for resource in executionResources:
        if resource['output'] == False:
            continue

        if not re.search(r'\.tif$', resource['path'], re.IGNORECASE):
            continue

        identifier = next((obj for obj in resource['data'] if obj['name'] == 'identifier'), None)["value"]
        spatial_resolution = next((obj for obj in resource['data'] if obj['name'] == 'pixelSize'), None)["value"]
        specificUsage = next((obj for obj in resource['data'] if obj['name'] == 'specificUsage'), None)["value"]

        # Datos para el XML
        layer_name = identifier
        title = identifier
        

      # JSON dinámico con los valores correspondientes
        wms_link = algoritm_result['executionResources'][0]['path']  # Link de WMS para este recurso específico
        layer_description = "Descripción de la capa generada"  # Puedes extraer o generar esto según el contexto
        file_identifier = "Ortomosaico_testeo"  # Un identificador único (se puede derivar)
        date_stamp = datetime.now().isoformat()
        publication_date = "2024-07-29"  # Basado en la fecha proporcionada en el archivo

        logging.info("Llamando a la función creador_xml_metadata.")
        
        # Generate XML tree
        tree = creador_xml_metadata(
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
    xml_encoded = base64.b64encode(xml_content).decode('utf-8')
    logging.info (f"Xml enconded {xml_encoded}")

    # Store the base64 encoded XML content in XCom
    return xml_encoded


 # JSON content as before
    # json_content = {
    #     'fileIdentifier': 'Ortomosaico_testeo',
    #     'dateStamp': datetime.now().isoformat(), cada uno 
    #     'title': 'Ortomosaico_0026_404_611271',
    #     'publicationDate': '2024-07-29',
    #     'boundingBox': {
    #         'westBoundLongitude': '-7.6392',
    #         'eastBoundLongitude': '-7.6336',
    #         'southBoundLatitude': '42.8025',
    #         'northBoundLatitude': '42.8044'
    #     },
    #     'spatialResolution': '0.026',  # Resolución espacial en metros
    #     'layerName': 'a__0026_4740004_611271',
    #     'layerDescription': 'Capa 0026 de prueba'
    # }


# URL para obtener las credenciales
credentials_url = "https://sgm.dev.cuatrodigital.com/geonetwork/credentials"

# Función para obtener las credenciales de GeoNetwork
def get_geonetwork_credentials():
    try:

        credential_dody = {
            "username" : "angel",
            "password" : "111111"
        }

        # Hacer la solicitud para obtener las credenciales
        logging.info(f"Obteniendo credenciales de: {credentials_url}")
        response = requests.post(credentials_url,json= credential_dody)

        # Verificar que la respuesta sea exitosa
        response.raise_for_status()

        # Extraer los headers y tokens necesarios
        response_object = response.json()
        access_token = response_object['accessToken']
        xsrf_token = response_object['xsrfToken']
        set_cookie_header = response_object['setCookieHeader']
        

        logging.info(f"Credenciales obtenidas: accessToken={access_token}, XSRF-TOKEN={xsrf_token}")

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
        xml_data = context['ti'].xcom_pull(task_ids='generate_xml')
        xml_decoded = base64.b64decode(xml_data).decode('utf-8')

        # Convertir el contenido XML a un objeto de tipo stream (equivalente a createReadStream en Node.js)
        xml_file_stream = io.StringIO(xml_decoded)

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
def creador_xml_metadata(file_identifier, specificUsage, organization_name, email_address, date_stamp, title, publication_date, west_bound, east_bound, south_bound, north_bound, spatial_resolution, protocol, wms_link, layer_name, layer_description):
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
    fid_cs.text = file_identifier

    # language
    language = ET.SubElement(root, "gmd:language")
    lang_code = ET.SubElement(language, "gmd:LanguageCode", {
        "codeList": "http://www.loc.gov/standards/iso639-2/",
        "codeListValue": "spa"
    })

    # characterSet
    char_set = ET.SubElement(root, "gmd:characterSet")
    char_set_code = ET.SubElement(char_set, "gmd:MD_CharacterSetCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_CharacterSetCode",
        "codeListValue": "utf8"
    })

    # parentIdentifier
    parent_identifier = ET.SubElement(root, "gmd:parentIdentifier", {"gco:nilReason": "missing"})

    # hierarchyLevel
    hierarchy = ET.SubElement(root, "gmd:hierarchyLevel")
    hierarchy_code = ET.SubElement(hierarchy, "gmd:MD_ScopeCode", {
        "codeList": "./resources/codelist.xml#MD_ScopeCode",
        "codeListValue": "dataset"
    })
    hierarchy_code.text = "dataset"

    # contact
    contact = ET.SubElement(root, "gmd:contact")
    responsible_party = ET.SubElement(contact, "gmd:CI_ResponsibleParty")
    org_name = ET.SubElement(responsible_party, "gmd:organisationName")
    org_name_cs = ET.SubElement(org_name, "gco:CharacterString")
    org_name_cs.text = organization_name

    # contact email
    contact_info = ET.SubElement(responsible_party, "gmd:contactInfo")
    ci_contact = ET.SubElement(contact_info, "gmd:CI_Contact")
    address = ET.SubElement(ci_contact, "gmd:address")
    ci_address = ET.SubElement(address, "gmd:CI_Address")
    email = ET.SubElement(ci_address, "gmd:electronicMailAddress")
    email_cs = ET.SubElement(email, "gco:CharacterString")
    email_cs.text = email_address

    # role
    role = ET.SubElement(responsible_party, "gmd:role")
    role_code = ET.SubElement(role, "gmd:CI_RoleCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_RoleCode",
        "codeListValue": "pointOfContact"
    })

    # dateStamp
    date_stamp_elem = ET.SubElement(root, "gmd:dateStamp")
    date_value = ET.SubElement(date_stamp_elem, "gco:DateTime")
    date_value.text = date_stamp

    # metadataStandardName
    metadata_standard = ET.SubElement(root, "gmd:metadataStandardName")
    metadata_standard_cs = ET.SubElement(metadata_standard, "gco:CharacterString")
    metadata_standard_cs.text = "NEM: ISO 19115:2003 + Reglamento (CE) Nº 1205/2008 de Inspire"

    # metadataStandardVersion
    metadata_version = ET.SubElement(root, "gmd:metadataStandardVersion")
    metadata_version_cs = ET.SubElement(metadata_version, "gco:CharacterString")
    metadata_version_cs.text = "1.2"

    # referenceSystemInfo
    ref_sys_info = ET.SubElement(root, "gmd:referenceSystemInfo")
    md_ref_sys = ET.SubElement(ref_sys_info, "gmd:MD_ReferenceSystem")
    ref_sys_id = ET.SubElement(md_ref_sys, "gmd:referenceSystemIdentifier")
    rs_id = ET.SubElement(ref_sys_id, "gmd:RS_Identifier")
    code = ET.SubElement(rs_id, "gmd:code")
    code_cs = ET.SubElement(code, "gco:CharacterString")
    code_cs.text = "EPSG:32629"
    code_space = ET.SubElement(rs_id, "gmd:codeSpace")
    code_space_cs = ET.SubElement(code_space, "gco:CharacterString")
    code_space_cs.text = "http://www.ign.es"

    # identificationInfo
    identification_info = ET.SubElement(root, "gmd:identificationInfo")
    md_data_identification = ET.SubElement(identification_info, "gmd:MD_DataIdentification")
    citation = ET.SubElement(md_data_identification, "gmd:citation")
    ci_citation = ET.SubElement(citation, "gmd:CI_Citation")
    title_elem = ET.SubElement(ci_citation, "gmd:title")
    title_cs = ET.SubElement(title_elem, "gco:CharacterString")
    title_cs.text = title

    # publication date
    pub_date = ET.SubElement(ci_citation, "gmd:date")
    ci_date = ET.SubElement(pub_date, "gmd:CI_Date")
    date = ET.SubElement(ci_date, "gmd:date")
    gco_date = ET.SubElement(date, "gco:Date")
    gco_date.text = publication_date
    date_type = ET.SubElement(ci_date, "gmd:dateType")
    ci_date_type = ET.SubElement(date_type, "gmd:CI_DateTypeCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_DateTypeCode",
        "codeListValue": "publication"
    })

    # bounding box
    extent = ET.SubElement(md_data_identification, "gmd:extent")
    ex_extent = ET.SubElement(extent, "gmd:EX_Extent")
    geographic_element = ET.SubElement(ex_extent, "gmd:geographicElement")
    bbox = ET.SubElement(geographic_element, "gmd:EX_GeographicBoundingBox")
    west_bound_elem = ET.SubElement(bbox, "gmd:westBoundLongitude")
    west_bound_cs = ET.SubElement(west_bound_elem, "gco:Decimal")
    west_bound_cs.text = west_bound
    east_bound_elem = ET.SubElement(bbox, "gmd:eastBoundLongitude")
    east_bound_cs = ET.SubElement(east_bound_elem, "gco:Decimal")
    east_bound_cs.text = east_bound
    south_bound_elem = ET.SubElement(bbox, "gmd:southBoundLatitude")
    south_bound_cs = ET.SubElement(south_bound_elem, "gco:Decimal")
    south_bound_cs.text = south_bound
    north_bound_elem = ET.SubElement(bbox, "gmd:northBoundLatitude")
    north_bound_cs = ET.SubElement(north_bound_elem, "gco:Decimal")
    north_bound_cs.text = north_bound

    # spatial resolution
    spatial_res = ET.SubElement(md_data_identification, "gmd:spatialResolution")
    distance = ET.SubElement(spatial_res, "gmd:MD_Resolution")
    dist_value = ET.SubElement(distance, "gmd:distance")
    dist_cs = ET.SubElement(dist_value, "gco:Distance", {"uom": "metros"})
    dist_cs.text = spatial_resolution

    # WMS linkage
    distribution_info = ET.SubElement(root, "gmd:distributionInfo")
    md_distribution = ET.SubElement(distribution_info, "gmd:MD_Distribution")
    transfer_options = ET.SubElement(md_distribution, "gmd:transferOptions")
    digital_transfer = ET.SubElement(transfer_options, "gmd:MD_DigitalTransferOptions")
    on_line = ET.SubElement(digital_transfer, "gmd:onLine")
    online_resource = ET.SubElement(on_line, "gmd:CI_OnlineResource")
    linkage = ET.SubElement(online_resource, "gmd:linkage")
    url = ET.SubElement(linkage, "gmd:URL")
    url.text = wms_link

    protocol_elem = ET.SubElement(online_resource, "gmd:protocol")
    protocol_cs = ET.SubElement(protocol_elem, "gco:CharacterString")
    protocol_cs.text = protocol

    name_elem = ET.SubElement(online_resource, "gmd:name")
    name_cs = ET.SubElement(name_elem, "gco:CharacterString")
    name_cs.text = layer_name

    desc_elem = ET.SubElement(online_resource, "gmd:description")
    desc_cs = ET.SubElement(desc_elem, "gco:CharacterString")
    desc_cs.text = layer_description

    resSpecific_usage = ET.SubElement(md_data_identification, "gmd:resourceSpecificUsage")
    usage = ET.SubElement(resSpecific_usage, "gmd:MD_Usage")
    specificUsageXml = ET.SubElement(usage, "gmd:specificUsage")
    specificUsageText = ET.SubElement(specificUsageXml, "gco:CharacterString")
    specificUsageText.text = specificUsage

    desc_cs = ET.SubElement(desc_elem, "gco:CharacterString")
    desc_cs.text = layer_description

    logging.info("XML creado correctamente.")
    
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

# Tarea 2: Subir el XML a GeoNetwork
upload_xml_task = PythonOperator(
    task_id='upload_to_geonetwork',
    python_callable=upload_to_geonetwork,
    provide_context=True,
    dag=dag
)

# Definir el flujo de las tareas
generate_xml_task >> upload_xml_task
