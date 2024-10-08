import base64
import os
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

# Función para generar un XML simple
def generate_xml_simple():
    logging.info("Generando XML básico.")
    
    root = ET.Element("metadata")
    file_identifier = ET.SubElement(root, "fileIdentifier")
    file_identifier.text = "Ortomosaico_testeo"
    
    title = ET.SubElement(root, "title")
    title.text = "Ortomosaico_0026_404_611271"
    
    date_stamp = ET.SubElement(root, "dateStamp")
    date_stamp.text = datetime.now().isoformat()

    organization = ET.SubElement(root, "organizationName")
    organization.text = "Instituto geográfico nacional (IGN)"
    
    bounding_box = ET.SubElement(root, "boundingBox")
    ET.SubElement(bounding_box, "westBoundLongitude").text = '-7.6392'
    ET.SubElement(bounding_box, "eastBoundLongitude").text = '-7.6336'
    ET.SubElement(bounding_box, "southBoundLatitude").text = '42.8025'
    ET.SubElement(bounding_box, "northBoundLatitude").text = '42.8044'
    
    # Convertir el XML a bytes
    xml_bytes_io = io.BytesIO()
    tree = ET.ElementTree(root)
    tree.write(xml_bytes_io, encoding='utf-8', xml_declaration=True)
    
    xml_content = xml_bytes_io.getvalue()

    # Codificar en base64
    xml_encoded = base64.b64encode(xml_content).decode('utf-8')
    return xml_encoded

# Función para obtener las credenciales de GeoNetwork
def get_geonetwork_credentials():
    try:
        credential_body = {
            "username": "angel",
            "password": "111111"
        }

        credentials_url = "https://sgm.dev.cuatrodigital.com/geonetwork/credentials"
        logging.info(f"Obteniendo credenciales de: {credentials_url}")
        
        response = requests.post(credentials_url, json=credential_body)
        response.raise_for_status()

        # Extraer los tokens necesarios
        response_object = response.json()
        return response_object['accessToken'], response_object['xsrfToken'], response_object['setCookieHeader']
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al obtener credenciales: {e}")
        raise Exception(f"Error al obtener credenciales: {e}")

# Función para subir el XML a GeoNetwork
def upload_to_geonetwork(**context):
    try:
        access_token, xsrf_token, set_cookie_header = get_geonetwork_credentials()
        
        # Obtener el XML base64 desde XCom
        xml_data = context['ti'].xcom_pull(task_ids='generate_xml')
        xml_decoded = base64.b64decode(xml_data).decode('utf-8')

        files = {
            'metadataType': (None, 'METADATA'),
            'uuidProcessing': (None, 'NOTHING'),
            'transformWith': (None, 'none'),
            'group': (None, 2),
            'category': (None, ''),
            'file': ('metadata.xml', xml_decoded, 'text/xml')
        }

        upload_url = f"{geonetwork_url}/records"
        headers = {
            'Authorization': f"Bearer {access_token}",
            'x-xsrf-token': xsrf_token,
            'Cookie': set_cookie_header[0]
        }

        logging.info(f"Subiendo XML a GeoNetwork en la URL: {upload_url}")
        response = requests.post(upload_url, headers=headers, files=files)
        response.raise_for_status()

        logging.info(f"Archivo subido correctamente a GeoNetwork. Respuesta: {response.text}")
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al subir el archivo a GeoNetwork: {e}")
        raise Exception(f"Error al subir el archivo a GeoNetwork: {e}")

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
    description='DAG para subir metadatos XML a GeoNetwork',
    schedule_interval=None,
    catchup=False
)

# Tarea 1: Generar el XML básico
generate_xml_task = PythonOperator(
    task_id='generate_xml',
    python_callable=generate_xml_simple,
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
