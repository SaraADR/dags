import base64
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import logging
import json
import io  # Para manejar el archivo XML en memoria

# Configurar la URL de GeoNetwork
geonetwork_url = "https://eiiob.dev.cuatrodigital.com/geonetwork/srv/api/records"

# Configurar el logging
logging.basicConfig(level=logging.INFO)

# Función para generar el XML
def generate_xml(algorithm_result, **context):
    logging.info("Iniciando la generación del XML.")
    
    # Obtenemos los datos del archivo de resultados del algoritmo
    json_content = {
        'fileIdentifier': algorithm_result['metadata'][0]['value'],  # ExecutionID
        'organizationName': 'Instituto geográfico nacional (IGN)',
        'email': 'ignis@organizacion.es',
        'dateStamp': datetime.now().isoformat(),
        'title': f"Ortomosaico_{algorithm_result['executionResources'][1]['data'][0]['value']}",  # identifier
        'publicationDate': algorithm_result['metadata'][2]['value'],  # citationDate
        'boundingBox': {
            'westBoundLongitude': algorithm_result['executionResources'][0]['data'][3]['value']['westBoundLongitude'],
            'eastBoundLongitude': algorithm_result['executionResources'][0]['data'][3]['value']['eastBoundLongitude'],
            'southBoundLatitude': algorithm_result['executionResources'][0]['data'][3]['value']['southBoundLatitude'],
            'northBoundLatitude': algorithm_result['executionResources'][0]['data'][3]['value']['nortBoundLatitude']
        },
        'spatialResolution': algorithm_result['executionResources'][1]['data'][3]['value'],  # pixelSize
        'protocol': 'OGC:WMS-1.3.0-http-get-map',
        'wmsLink': 'https://geoserver.dev.cuatrodigital.com/geoserver/tests-geonetwork/wms',
        'layerName': algorithm_result['executionResources'][1]['data'][0]['value'],  # identifier
        'layerDescription': algorithm_result['executionResources'][1]['data'][2]['value']  # specificUsage
    }

    logging.info(f"Contenido JSON cargado: {json_content}")

    # Crear el árbol XML
    root = ET.Element('gmd:MD_Metadata')

    file_id_element = ET.SubElement(root, 'gmd:fileIdentifier')
    file_id_value = ET.SubElement(file_id_element, 'gco:CharacterString')
    file_id_value.text = json_content['fileIdentifier']

    title_element = ET.SubElement(root, 'gmd:title')
    title_value = ET.SubElement(title_element, 'gco:CharacterString')
    title_value.text = json_content['title']

    org_element = ET.SubElement(root, 'gmd:organisationName')
    org_value = ET.SubElement(org_element, 'gco:CharacterString')
    org_value.text = json_content['organizationName']

    email_element = ET.SubElement(root, 'gmd:contactInfo')
    email_value = ET.SubElement(email_element, 'gco:CharacterString')
    email_value.text = json_content['email']

    bounding_box_element = ET.SubElement(root, 'gmd:EX_GeographicBoundingBox')
    west_bound = ET.SubElement(bounding_box_element, 'gmd:westBoundLongitude')
    west_bound_value = ET.SubElement(west_bound, 'gco:Decimal')
    west_bound_value.text = json_content['boundingBox']['westBoundLongitude']

    east_bound = ET.SubElement(bounding_box_element, 'gmd:eastBoundLongitude')
    east_bound_value = ET.SubElement(east_bound, 'gco:Decimal')
    east_bound_value.text = json_content['boundingBox']['eastBoundLongitude']

    south_bound = ET.SubElement(bounding_box_element, 'gmd:southBoundLatitude')
    south_bound_value = ET.SubElement(south_bound, 'gco:Decimal')
    south_bound_value.text = json_content['boundingBox']['southBoundLatitude']

    north_bound = ET.SubElement(bounding_box_element, 'gmd:northBoundLatitude')
    north_bound_value = ET.SubElement(north_bound, 'gco:Decimal')
    north_bound_value.text = json_content['boundingBox']['northBoundLatitude']

    # Convertir el árbol XML a una cadena
    xml_data = ET.tostring(root, encoding='utf-8').decode()

    # Regresar la cadena XML generada
    return xml_data

# Función para realizar el POST request a GeoNetwork con autenticación
def post_to_geonetwork(xml_data, username, password, **context):
    logging.info("Iniciando POST request a GeoNetwork.")

    headers = {
        'Content-Type': 'application/xml'
    }

    # Haciendo la solicitud POST con autenticación
    response = requests.post(
        geonetwork_url, 
        data=xml_data, 
        headers=headers, 
        auth=(username, password)
    )

    if response.status_code == 201:
        logging.info("Archivo subido correctamente a GeoNetwork.")
    else:
        logging.error(f"Error al subir el archivo: {response.status_code}, {response.text}")

    return response.status_code

# Definir el DAG de Airflow
with DAG(dag_id='metashape_rgb',
         schedule_interval=None,
         start_date=datetime(2024, 10, 1),
         catchup=False) as dag:

    # Tarea para generar el XML
    generate_xml_task = PythonOperator(
        task_id='generate_xml_task',
        python_callable=generate_xml,
        op_kwargs={'algorithm_result': {
            # Aquí cargamos el contenido del archivo algorithm_result.json
            "metadata": [
                {"name": "ExecutionID", "value": "a2f18ca5-d232-11ee-8201-40ec99c6d521"}
            ],
            "executionResources": [
                {
                    "data": [
                        {},
                        {},
                        {},
                        {"value": {"westBoundLongitude": "611271.6716", "eastBoundLongitude": "611725.0085",
                                   "southBoundLatitude": "4739776.3222", "nortBoundLatitude": "4740004.5330"}}
                    ]
                },
                {
                    "data": [
                        {"value": "Ortomosaico_0026_4740004_611271"},
                        {},
                        {"value": "Conocimiento y estudio con precisión de la ubicación"},
                        {"value": 0.026}
                    ]
                }
            ]
        }}
    )

    # Tarea para subir el archivo a GeoNetwork
    post_to_geonetwork_task = PythonOperator(
        task_id='post_to_geonetwork_task',
        python_callable=post_to_geonetwork,
        op_kwargs={
            'xml_data': "{{ task_instance.xcom_pull(task_ids='generate_xml_task') }}",
            'username': 'your_username_here',  # Reemplazar con el usuario
            'password': 'your_password_here'   # Reemplazar con la contraseña
        }
    )

    generate_xml_task >> post_to_geonetwork_task
