from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import xml.etree.ElementTree as ET
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
with DAG(
    'generate_and_upload_xml',
    default_args=default_args,
    description='Genera un archivo XML y lo sube a GeoServer',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    def generate_xml():
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

        # Crear estructura XML
        root = ET.Element('metadata')
        ET.SubElement(root, 'fileIdentifier').text = file_identifier
        ET.SubElement(root, 'organizationName').text = organization_name
        ET.SubElement(root, 'email').text = email_address
        ET.SubElement(root, 'dateStamp').text = date_stamp
        ET.SubElement(root, 'title').text = title
        ET.SubElement(root, 'publicationDate').text = publication_date
        bounding_box = ET.SubElement(root, 'boundingBox')
        ET.SubElement(bounding_box, 'westBoundLongitude').text = west_bound
        ET.SubElement(bounding_box, 'eastBoundLongitude').text = east_bound
        ET.SubElement(bounding_box, 'southBoundLatitude').text = south_bound
        ET.SubElement(bounding_box, 'northBoundLatitude').text = north_bound
        ET.SubElement(root, 'spatialResolution').text = spatial_resolution
        service_identification = ET.SubElement(root, 'serviceIdentification')
        ET.SubElement(service_identification, 'protocol').text = protocol
        ET.SubElement(service_identification, 'wmsLink').text = wms_link
        ET.SubElement(service_identification, 'layerName').text = layer_name
        ET.SubElement(service_identification, 'layerDescription').text = layer_description

        # Guardar XML
        tree = ET.ElementTree(root)
        xml_file = f"{file_identifier}.xml"
        tree.write(xml_file)

        print(f"Archivo XML generado: {xml_file}")
        return xml_file

    def upload_to_geoserver(**kwargs):
        # Obtener el nombre del archivo generado de la tarea anterior
        ti = kwargs['ti']
        xml_file = ti.xcom_pull(task_ids='generate_xml')

        # Credenciales
        auth_url = "https://eiiob.dev.cuatrodigital.com/geonetwork/srv/api/"
        credentials = {
            "username": "angel",
            "password": "111111"
        }

        # Obtener el token de autenticación
        response = requests.post(auth_url, json=credentials)

        if response.status_code == 200:
            token = response.json().get('token')
            print(f"Token obtenido: {token}")

            # Subir el archivo XML
            headers = {
                "Authorization": f"Bearer {token}"
            }
            files = {'file': open(xml_file, 'rb')}
            upload_url = "https://eiiob.dev.cuatrodigital.com/geonetwork/srv/api/"

            upload_response = requests.post(upload_url, headers=headers, files=files)

            if upload_response.status_code == 200:
                print(f"Archivo {xml_file} subido correctamente a GeoServer")
            else:
                print(f"Error al subir el archivo: {upload_response.content}")
        else:
            print(f"Error de autenticación: {response.content}")
            

    # Definir tareas usando PythonOperator
    task_generate_xml = PythonOperator(
        task_id='generate_xml',
        python_callable=generate_xml
    )

    task_upload_to_geoserver = PythonOperator(
        task_id='upload_to_geoserver',
        python_callable=upload_to_geoserver,
        provide_context=True
    )

    # Definir el flujo de tareas
    task_generate_xml >> task_upload_to_geoserver
