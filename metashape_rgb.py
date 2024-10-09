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

# Configurar la URL de GeoNetwork
geonetwork_url = "https://eiiob.dev.cuatrodigital.com/geonetwork/srv/api"

# Configurar el logging
logging.basicConfig(level=logging.INFO)

# Función para generar el XML
def generate_xml(**context):
    logging.info("Iniciando la generación del XML.")
    
    # JSON content as before
    json_content = {
        'fileIdentifier': 'Ortomosaico_testeo',
        'organizationName': 'Instituto geográfico nacional (IGN)',
        'email': 'ignis@organizacion.es',
        'dateStamp': datetime.now().isoformat(),
        'title': 'Ortomosaico_0026_404_611271',
        'publicationDate': '2024-07-29',
        'boundingBox': {
            'westBoundLongitude': '-7.6392',
            'eastBoundLongitude': '-7.6336',
            'southBoundLatitude': '42.8025',
            'northBoundLatitude': '42.8044'
        },
        'spatialResolution': '0.026',
        'protocol': 'OGC:WMS-1.3.0-http-get-map',
        'wmsLink': 'https://geoserver.dev.cuatrodigital.com/geoserver/tests-geonetwork/wms',
        'layerName': 'a__0026_4740004_611271',
        'layerDescription': 'Capa 0026 de prueba'
    }

    logging.info(f"Contenido JSON cargado: {json_content}")

    # Creación del XML
    root = ET.Element("gmd:MD_Metadata")

    # fileIdentifier
    file_id_elem = ET.SubElement(root, "gmd:fileIdentifier")
    char_str = ET.SubElement(file_id_elem, "gco:CharacterString")
    char_str.text = json_content['fileIdentifier']

    # organizationName
    org_name_elem = ET.SubElement(root, "gmd:organizationName")
    org_name_str = ET.SubElement(org_name_elem, "gco:CharacterString")
    org_name_str.text = json_content['organizationName']

    # email
    contact_info = ET.SubElement(root, "gmd:contact")
    contact_org = ET.SubElement(contact_info, "gmd:CI_ResponsibleParty")
    contact_email = ET.SubElement(contact_org, "gmd:contactInfo")
    contact_email_elem = ET.SubElement(contact_email, "gmd:CI_Contact")
    email_address = ET.SubElement(contact_email_elem, "gmd:address")
    address_char = ET.SubElement(email_address, "gco:CharacterString")
    address_char.text = json_content['email']

    # dateStamp
    date_elem = ET.SubElement(root, "gmd:dateStamp")
    date_char = ET.SubElement(date_elem, "gco:Date")
    date_char.text = json_content['dateStamp']

    # title
    title_elem = ET.SubElement(root, "gmd:title")
    title_char = ET.SubElement(title_elem, "gco:CharacterString")
    title_char.text = json_content['title']

    # publicationDate
    pub_date_elem = ET.SubElement(root, "gmd:publicationDate")
    pub_date_char = ET.SubElement(pub_date_elem, "gco:Date")
    pub_date_char.text = json_content['publicationDate']

    # boundingBox
    bbox_elem = ET.SubElement(root, "gmd:EX_GeographicBoundingBox")
    west_bound_elem = ET.SubElement(bbox_elem, "gmd:westBoundLongitude")
    west_bound_char = ET.SubElement(west_bound_elem, "gco:Decimal")
    west_bound_char.text = json_content['boundingBox']['westBoundLongitude']

    east_bound_elem = ET.SubElement(bbox_elem, "gmd:eastBoundLongitude")
    east_bound_char = ET.SubElement(east_bound_elem, "gco:Decimal")
    east_bound_char.text = json_content['boundingBox']['eastBoundLongitude']

    south_bound_elem = ET.SubElement(bbox_elem, "gmd:southBoundLatitude")
    south_bound_char = ET.SubElement(south_bound_elem, "gco:Decimal")
    south_bound_char.text = json_content['boundingBox']['southBoundLatitude']

    north_bound_elem = ET.SubElement(bbox_elem, "gmd:northBoundLatitude")
    north_bound_char = ET.SubElement(north_bound_elem, "gco:Decimal")
    north_bound_char.text = json_content['boundingBox']['northBoundLatitude']

    # spatialResolution
    res_elem = ET.SubElement(root, "gmd:spatialResolution")
    res_char = ET.SubElement(res_elem, "gco:Decimal")
    res_char.text = json_content['spatialResolution']

    # wmsLink
    distribution_info = ET.SubElement(root, "gmd:distributionInfo")
    md_distribution = ET.SubElement(distribution_info, "gmd:MD_Distribution")
    transfer_options = ET.SubElement(md_distribution, "gmd:transferOptions")
    digital_transfer = ET.SubElement(transfer_options, "gmd:MD_DigitalTransferOptions")
    on_line = ET.SubElement(digital_transfer, "gmd:onLine")
    online_resource = ET.SubElement(on_line, "gmd:CI_OnlineResource")
    linkage = ET.SubElement(online_resource, "gmd:linkage")
    url = ET.SubElement(linkage, "gmd:URL")
    url.text = json_content['wmsLink']

    # protocol
    protocol_elem = ET.SubElement(online_resource, "gmd:protocol")
    protocol_cs = ET.SubElement(protocol_elem, "gco:CharacterString")
    protocol_cs.text = json_content['protocol']

    # layerName
    name_elem = ET.SubElement(online_resource, "gmd:name")
    name_cs = ET.SubElement(name_elem, "gco:CharacterString")
    name_cs.text = json_content['layerName']

    # layerDescription
    desc_elem = ET.SubElement(online_resource, "gmd:description")
    desc_cs = ET.SubElement(desc_elem, "gco:CharacterString")
    desc_cs.text = json_content['layerDescription']

    logging.info("XML creado correctamente.")
    
    return ET.ElementTree(root)

# Nueva función para guardar el XML en un archivo
def save_xml(**context):
    logging.info("Guardando el XML a un archivo local.")
    xml_tree = context['ti'].xcom_pull(task_ids='generate_xml')
    
    # Guardar el archivo en una ubicación temporal
    file_path = '/tmp/generated_metadata.xml'
    xml_tree.write(file_path, encoding='utf-8', xml_declaration=True)
    
    logging.info(f"XML guardado en {file_path}.")
    return file_path

# Función para subir el XML a GeoNetwork
def upload_to_geonetwork(**context):
    logging.info("Iniciando la subida del XML a GeoNetwork.")
    file_path = context['ti'].xcom_pull(task_ids='save_xml')
    
    # Aquí se usaría la URL de GeoNetwork y la autenticación
    files = {'file': open(file_path, 'rb')}
    response = requests.post(f"{geonetwork_url}/records", files=files)
    
    if response.status_code == 200:
        logging.info("Archivo subido correctamente.")
    else:
        logging.error(f"Error en la subida: {response.status_code} - {response.text}")

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
    schedule_interval=None,
    catchup=False
)

# Tarea 1: Generar el XML
generate_xml_task = PythonOperator(
    task_id='generate_xml',
    python_callable=generate_xml,
    provide_context=True,
    dag=dag
)

# Tarea 2: Guardar el XML
save_xml_task = PythonOperator(
    task_id='save_xml',
    python_callable=save_xml,
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
generate_xml_task >> save_xml_task >> upload_xml_task
