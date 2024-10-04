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
geonetwork_url = "https://eiiob.dev.cuatrodigital.com/geonetwork/srv/api/"

# Configurar el logging
logging.basicConfig(level=logging.INFO)

import base64

def generate_xml(**context):
    logging.info("Iniciando la generación del XML.")
    
    # Simulamos la configuración que normalmente vendría de Airflow o algún input
    json_content = {
        'fileIdentifier': 'Ortomosaico_testeo',
        'organizationName': 'Instituto geográfico nacional (IGN)',
        'email': 'ignis@organizacion.es',
        'dateStamp': datetime.now().isoformat(),
        'title': 'Ortomosaico_0026_4740004_611271',
        'publicationDate': '2024-07-29',
        'boundingBox': {
            'westBoundLongitude': '-7.6392',
            'eastBoundLongitude': '-7.6336',
            'southBoundLatitude': '42.8025',
            'northBoundLatitude': '42.8044'
        },
        'spatialResolution': '0.026',  # Resolución espacial en metros
        'protocol': 'OGC:WMS-1.3.0-http-get-map',
        'wmsLink': 'https://geoserver.dev.cuatrodigital.com/geoserver/tests-geonetwork/wms',
        'layerName': 'a__0026_4740004_611271',
        'layerDescription': 'Capa 0026 de prueba'
    }

    logging.info(f"Contenido JSON cargado: {json_content}")

    # Generar el árbol XML
    tree = creador_xml_metadata(
        file_identifier=json_content['fileIdentifier'],
        organization_name=json_content['organizationName'],
        email_address=json_content['email'],
        date_stamp=json_content['dateStamp'],
        title=json_content['title'],
        publication_date=json_content['publicationDate'],
        west_bound=json_content['boundingBox']['westBoundLongitude'],
        east_bound=json_content['boundingBox']['eastBoundLongitude'],
        south_bound=json_content['boundingBox']['southBoundLatitude'],
        north_bound=json_content['boundingBox']['northBoundLatitude'],
        spatial_resolution=json_content['spatialResolution'],
        protocol=json_content['protocol'],
        wms_link=json_content['wmsLink'],
        layer_name=json_content['layerName'],
        layer_description=json_content['layerDescription']
    )

    if tree is None:
        logging.error("La función creador_xml_metadata retornó None. Asegúrate de que está retornando un ElementTree válido.")
        raise Exception("Error: creador_xml_metadata retornó None.")

    logging.info("El XML ha sido creado exitosamente en memoria.")

    # Convertir el árbol XML a una cadena de texto (en memoria)
    xml_bytes_io = io.BytesIO()
    tree.write(xml_bytes_io, encoding='utf-8', xml_declaration=True)
    xml_content = xml_bytes_io.getvalue()

    # Codificar el contenido XML a base64 para que sea serializable por JSON
    xml_base64 = base64.b64encode(xml_content).decode('utf-8')

    # Guardar el contenido XML en XCom para la siguiente tarea
    return xml_base64


# Función para subir el XML a GeoNetwork o EEIOB directamente
def upload_to_geonetwork(**context):
    logging.info("Iniciando la subida del archivo XML directamente a GeoNetwork.")
    
     # Obtener el XML codificado desde XCom
    xml_encoded = context['ti'].xcom_pull(task_ids='generate_xml')

    # Decodificar el XML de base64 a bytes
    xml_content = base64.b64decode(xml_encoded)

    # Construir la URL de subida
    url = f"{geonetwork_url}/records"  # Endpoint para subir los registros a GeoNetwork

    # Credenciales de usuario y contraseña proporcionadas
    auth = ('angel', '111111')  # Usuario y contraseña para autenticación

    # Headers para la solicitud
    headers = {
        'Content-Type': 'application/xml',
    }

    # Hacer la solicitud POST para subir el archivo XML a GeoNetwork
    try:
        logging.info(f"Subiendo XML a la URL: {url}")
        response = requests.post(url, headers=headers, data=xml_content, auth=auth)
        response.raise_for_status()  # Levanta una excepción para códigos de error HTTP
        logging.info(f"Archivo subido correctamente a GeoNetwork. Respuesta: {response.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al subir el archivo a GeoNetwork: {e}")
        raise Exception(f"Error al subir el archivo a GeoNetwork: {e}")

# Función para crear el XML (del código original)
def creador_xml_metadata(file_identifier, organization_name, email_address, date_stamp, title, publication_date, west_bound, east_bound, south_bound, north_bound, spatial_resolution, protocol, wms_link, layer_name, layer_description):
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

    # Crear los elementos XML de manera similar a lo que ya tenías en el código original
    # (fileIdentifier, boundingBox, etc.)

    logging.info("XML creado correctamente.")
    
    return ET.ElementTree(root)  # Asegurarse de que retornamos un ElementTree

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
