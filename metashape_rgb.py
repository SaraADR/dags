import os
import xml.etree.ElementTree as ET
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
import requests
import json

# Función para generar el XML
def generate_xml(**context):
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

    # Parámetros XML
    file_identifier = json_content['fileIdentifier']
    organization_name = json_content['organizationName']
    email_address = json_content['email']
    date_stamp = json_content['dateStamp']
    title = json_content['title']
    publication_date = json_content['publicationDate']
    west_bound = json_content['boundingBox']['westBoundLongitude']
    east_bound = json_content['boundingBox']['eastBoundLongitude']
    south_bound = json_content['boundingBox']['southBoundLatitude']
    north_bound = json_content['boundingBox']['northBoundLatitude']
    spatial_resolution = json_content['spatialResolution']
    protocol = json_content['protocol']
    wms_link = json_content['wmsLink']
    layer_name = json_content['layerName']
    layer_description = json_content['layerDescription']

    # Directorio fijo para guardar el XML
    output_dir = "/tmp/airflow_output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    xml_file_path = os.path.join(output_dir, "archivo.xml")

    # Crear el contenido del XML usando la función creador_xml_metadata
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
        protocol=protocol,
        wms_link=wms_link,
        layer_name=layer_name,
        layer_description=layer_description
    )

    # Guardar el archivo XML en la ubicación fija
    tree.write(xml_file_path, encoding='utf-8', xml_declaration=True)

    # Retornar la ruta del archivo generado para la siguiente tarea
    return xml_file_path

# Función para subir el XML a GeoNetwork o EEIOB
def upload_to_geonetwork(xml_file_path, **context):
    # Extraer la conexión desde Airflow
    connection = BaseHook.get_connection('geonetwork_connection')  # Aquí se asume que ya existe una conexión configurada en Airflow
    url = connection.host
    headers = {
        'Content-Type': 'application/xml',
        'Authorization': f'Bearer {connection.password}'  # O cualquier otro método de autenticación según la configuración
    }

    # Leer el contenido del archivo XML
    with open(xml_file_path, 'r') as file:
        xml_content = file.read()

    # Hacer la solicitud POST para subir el archivo a GeoNetwork
    response = requests.post(url, headers=headers, data=xml_content)

    # Manejar la respuesta
    if response.status_code == 200:
        print(f"Archivo subido correctamente a GeoNetwork: {response.text}")
    else:
        print(f"Error al subir el archivo a GeoNetwork: {response.status_code}, {response.text}")
        raise Exception(f"Failed to upload XML to GeoNetwork: {response.text}")

# Función para crear el XML (del código original)
def creador_xml_metadata(file_identifier, organization_name, email_address, date_stamp, title, publication_date, west_bound, east_bound, south_bound, north_bound, spatial_resolution, protocol, wms_link, layer_name, layer_description):
    # ... (el mismo código que en el script original para generar el XML)
    pass

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
    schedule_interval=None,  # Se puede ajustar según necesidades (ej: '@daily')
    catchup=False
)

# Tarea 1: Generar XML
generate_xml_task = PythonOperator(
    task_id='generate_xml',
    python_callable=generate_xml,
    provide_context=True,
    dag=dag
)

# Tarea 2: Subir el archivo XML a GeoNetwork
upload_xml_task = PythonOperator(
    task_id='upload_to_geonetwork',
    python_callable=upload_to_geonetwork,
    provide_context=True,
    op_kwargs={'xml_file_path': '{{ ti.xcom_pull(task_ids="generate_xml") }}'},
    dag=dag
)

# Definir el flujo de las tareas
generate_xml_task >> upload_xml_task
