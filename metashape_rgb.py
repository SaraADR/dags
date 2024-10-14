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

    logging.info(f"Convirtiendo coordenadas de EPSG:{epsg_input} a EPSG:4326.")
    logging.info(f"Coordenadas antes de la conversión: sur={south}, oeste={west}, norte={north}, este={east}")
    
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

    logging.info(f"Coordenadas después de la conversión: sur={south2}, oeste={west2}, norte={north2}, este={east2}")


    return south2, west2, north2, east2

    

# Función para generar el XML
def generate_xml(**kwargs):
    logging.info("Iniciando la generación del XML.")

    xml_encoded = []
    
    algoritm_result = kwargs['dag_run'].conf.get('json')

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
    email_address = 'avincis@organizacion.es'
    protocol = 'OGC:WMS-1.3.0-http-get-map'
    wms_link = 'https://geoserver.dev.cuatrodigital.com/geoserver/tests-geonetwork/wms'

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
        xml_data_array = context['ti'].xcom_pull(task_ids='generate_xml')

        for xml_data in xml_data_array:
        
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
def creador_xml_metadata(file_identifier, specificUsage, wmsLayer, organization_name, email_address, date_stamp, title, publication_date, west_bound, east_bound, south_bound, north_bound, spatial_resolution, protocol, wms_link, layer_name, layer_description):
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

     # Añadir language
    language = ET.SubElement(root, "gmd:language")
    lang_code = ET.SubElement(language, "gmd:LanguageCode", {
        "codeList": "http://www.loc.gov/standards/iso639-2/",
        "codeListValue": "spa"
    })

    # Añadir characterSet
    char_set = ET.SubElement(root, "gmd:characterSet")
    char_set_code = ET.SubElement(char_set, "gmd:MD_CharacterSetCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_CharacterSetCode",
        "codeListValue": "utf8"
    })

    # Añadir parentIdentifier
    parent_identifier = ET.SubElement(root, "gmd:parentIdentifier", {"gco:nilReason": "missing"})

    # Añadir hierarchyLevel
    hierarchy = ET.SubElement(root, "gmd:hierarchyLevel")
    hierarchy_code = ET.SubElement(hierarchy, "gmd:MD_ScopeCode", {
        "codeList": "./resources/codelist.xml#MD_ScopeCode",
        "codeListValue": "dataset"
    })
    hierarchy_code.text = "dataset"


        # Añadir gmd:descriptiveKeywords al root
    gmd_descriptiveKeywords = ET.SubElement(root, "gmd:descriptiveKeywords")

    # Añadir gmd:MD_Keywords dentro de descriptiveKeywords
    gmd_MD_Keywords = ET.SubElement(gmd_descriptiveKeywords, "gmd:MD_Keywords")

    # Añadir gmd:keyword y gco:CharacterString dentro de MD_Keywords para cada keyword
    keywords = ["photogrametry", "burst", "orthomosaic", "RGB", "aerial-photography", "Opendata"]

    for keyword in keywords:
        gmd_keyword = ET.SubElement(gmd_MD_Keywords, "gmd:keyword")
        gco_CharacterString = ET.SubElement(gmd_keyword, "gco:CharacterString")
        gco_CharacterString.text = keyword

    # Añadir gmd:type dentro de MD_Keywords
    gmd_type = ET.SubElement(gmd_MD_Keywords, "gmd:type")
    gmd_MD_KeywordTypeCode = ET.SubElement(gmd_type, "gmd:MD_KeywordTypeCode", {
        "codeList": "https://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_KeywordTypeCode",
        "codeListValue": "theme"
    })

    # Añadir gmd:thesaurusName dentro de MD_Keywords
    gmd_thesaurusName = ET.SubElement(gmd_MD_Keywords, "gmd:thesaurusName")

    # Añadir gmd:CI_Citation dentro de thesaurusName
    gmd_CI_Citation = ET.SubElement(gmd_thesaurusName, "gmd:CI_Citation")

    # Añadir gmd:title dentro de CI_Citation
    gmd_title = ET.SubElement(gmd_CI_Citation, "gmd:title")
    gmx_Anchor = ET.SubElement(gmd_title, "gmx:Anchor", {
        "xlink:href": "http://www.eionet.europa.eu/gemet/inspire_themes"
    })
    gmx_Anchor.text = "Keywords"

    # Añadir gmd:date dentro de CI_Citation
    gmd_date = ET.SubElement(gmd_CI_Citation, "gmd:date")
    gmd_CI_Date = ET.SubElement(gmd_date, "gmd:CI_Date")

    # Añadir gmd:date y gco:Date dentro de CI_Date
    gmd_date_inner = ET.SubElement(gmd_CI_Date, "gmd:date")
    gco_Date = ET.SubElement(gmd_date_inner, "gco:Date")
    gco_Date.text = "2008-06-01"

    # Añadir gmd:dateType dentro de CI_Date
    gmd_dateType = ET.SubElement(gmd_CI_Date, "gmd:dateType")
    gmd_CI_DateTypeCode = ET.SubElement(gmd_dateType, "gmd:CI_DateTypeCode", {
        "codeList": "https://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_DateTypeCode",
        "codeListValue": "publication"
    })


    # Añadir contact
    contact = ET.SubElement(root, "gmd:contact")
    responsible_party = ET.SubElement(contact, "gmd:CI_ResponsibleParty")
    org_name = ET.SubElement(responsible_party, "gmd:organisationName")
    org_name_cs = ET.SubElement(org_name, "gco:CharacterString")
    org_name_cs.text = str(organization_name)

    # Añadir contactInfo
    contact_info = ET.SubElement(responsible_party, "gmd:contactInfo")
    ci_contact = ET.SubElement(contact_info, "gmd:CI_Contact")
    address = ET.SubElement(ci_contact, "gmd:address")
    ci_address = ET.SubElement(address, "gmd:CI_Address")
    email = ET.SubElement(ci_address, "gmd:electronicMailAddress")
    email_cs = ET.SubElement(email, "gco:CharacterString")
    email_cs.text = str(email_address)

    # Añadir role
    role = ET.SubElement(responsible_party, "gmd:role")
    role_code = ET.SubElement(role, "gmd:CI_RoleCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_RoleCode",
        "codeListValue": "pointOfContact"
    })

    # Añadir dateStamp
    date_stamp_elem = ET.SubElement(root, "gmd:dateStamp")
    date_value = ET.SubElement(date_stamp_elem, "gco:DateTime")
    date_value.text = date_stamp

    # Añadir metadataStandardName
    metadata_standard = ET.SubElement(root, "gmd:metadataStandardName")
    metadata_standard_cs = ET.SubElement(metadata_standard, "gco:CharacterString")
    metadata_standard_cs.text = "NEM: ISO 19115:2003 + Reglamento (CE) Nº 1205/2008 de Inspire"

    # Añadir metadataStandardVersion
    metadata_version = ET.SubElement(root, "gmd:metadataStandardVersion")
    metadata_version_cs = ET.SubElement(metadata_version, "gco:CharacterString")
    metadata_version_cs.text = "1.2"

    # Añadir referenceSystemInfo
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

    # Añadir identificationInfo
    identification_info = ET.SubElement(root, "gmd:identificationInfo")
    md_data_identification = ET.SubElement(identification_info, "gmd:MD_DataIdentification")
    citation = ET.SubElement(md_data_identification, "gmd:citation")
    ci_citation = ET.SubElement(citation, "gmd:CI_Citation")
    title_elem = ET.SubElement(ci_citation, "gmd:title")
    title_cs = ET.SubElement(title_elem, "gco:CharacterString")
    title_cs.text = str(file_identifier)

    # Añadir publication date
    pub_date = ET.SubElement(ci_citation, "gmd:date")
    ci_date = ET.SubElement(pub_date, "gmd:CI_Date")
    date = ET.SubElement(ci_date, "gmd:date")
    gco_date = ET.SubElement(date, "gco:Date")
    gco_date.text = str(publication_date)
    date_type = ET.SubElement(ci_date, "gmd:dateType")
    ci_date_type = ET.SubElement(date_type, "gmd:CI_DateTypeCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_DateTypeCode",
        "codeListValue": "publication"
    })

    # Añadir bounding box
    extent = ET.SubElement(md_data_identification, "gmd:extent")
    ex_extent = ET.SubElement(extent, "gmd:EX_Extent")
    geographic_element = ET.SubElement(ex_extent, "gmd:geographicElement")
    bbox = ET.SubElement(geographic_element, "gmd:EX_GeographicBoundingBox")
    west_bound_elem = ET.SubElement(bbox, "gmd:westBoundLongitude")
    west_bound_cs = ET.SubElement(west_bound_elem, "gco:Decimal")
    west_bound_cs.text = str(west_bound)
    east_bound_elem = ET.SubElement(bbox, "gmd:eastBoundLongitude")
    east_bound_cs = ET.SubElement(east_bound_elem, "gco:Decimal")
    east_bound_cs.text = str(east_bound)
    south_bound_elem = ET.SubElement(bbox, "gmd:southBoundLatitude")
    south_bound_cs = ET.SubElement(south_bound_elem, "gco:Decimal")
    south_bound_cs.text = str(south_bound)
    north_bound_elem = ET.SubElement(bbox, "gmd:northBoundLatitude")
    north_bound_cs = ET.SubElement(north_bound_elem, "gco:Decimal")
    north_bound_cs.text = str(north_bound)

    # Añadir spatial resolution
    spatial_res = ET.SubElement(md_data_identification, "gmd:spatialResolution")
    distance = ET.SubElement(spatial_res, "gmd:MD_Resolution")
    dist_value = ET.SubElement(distance, "gmd:distance")
    dist_cs = ET.SubElement(dist_value, "gco:Distance", {"uom": "metros"})
    dist_cs.text = str(spatial_resolution)

    # Añadir topicCategory
    topic_category = ET.SubElement(md_data_identification, "gmd:topicCategory")
    topic_code = ET.SubElement(topic_category, "gmd:MD_TopicCategoryCode")
    topic_code.text = "imageryBaseMapsEarthCover"

    # Añadir resourceSpecificUsage
    resSpecific_usage = ET.SubElement(md_data_identification, "gmd:resourceSpecificUsage")
    usage = ET.SubElement(resSpecific_usage, "gmd:MD_Usage")
    specificUsageXml = ET.SubElement(usage, "gmd:specificUsage")
    specificUsageText = ET.SubElement(specificUsageXml, "gco:CharacterString")
    specificUsageText.text = str(specificUsage)

    # Añadir DQ_DataQuality
    dq_data_quality = ET.SubElement(root, "gmd:DQ_DataQuality")
    scope = ET.SubElement(dq_data_quality, "gmd:scope")
    dq_scope = ET.SubElement(scope, "gmd:DQ_Scope")
    level = ET.SubElement(dq_scope, "gmd:level")
    md_scope_code = ET.SubElement(level, "gmd:MD_ScopeCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_ScopeCode",
        "codeListValue": "dataset"
    })
    md_scope_code.text = "dataset"

    # Añadir lineage
    lineage = ET.SubElement(dq_data_quality, "gmd:lineage")
    li_lineage = ET.SubElement(lineage, "gmd:LI_Lineage")
    statement = ET.SubElement(li_lineage, "gmd:statement")
    statement_cs = ET.SubElement(statement, "gco:CharacterString")
    statement_cs.text = "Ortomosaico generado mediante el procesamiento de imágenes aéreas."

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
