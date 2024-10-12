import requests
import logging
import base64
import io
import xml.etree.ElementTree as ET
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# URL para obtener las credenciales
credentials_url = "https://sgm.dev.cuatrodigital.com/geonetwork/credentials"

# Función para obtener las credenciales de GeoNetwork
def get_geonetwork_credentials():
    try:
        credential_body = {
            "username" : "angel",
            "password" : "111111"
        }

        # Hacer la solicitud para obtener las credenciales
        logging.info(f"Obteniendo credenciales de: {credentials_url}")
        response = requests.post(credentials_url, json=credential_body)

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

        # Convertir el contenido XML a un objeto de tipo stream
        xml_file_stream = io.StringIO(xml_decoded)

        logging.info(f"XML DATA: {xml_data}")
        logging.info(xml_decoded)

        # Datos para subir a GeoNetwork
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
            'Authorization': f"Bearer {access_token}",
            'x-xsrf-token': str(xsrf_token),
            'Cookie': str(set_cookie_header[0]),
            'Accept': 'application/json'
        }

        # Realizar la solicitud POST para subir el archivo XML
        logging.info(f"Subiendo XML a la URL: {upload_url}")
        response = requests.post(upload_url, files=files, data=data, headers=headers)
        logging.info(response)

        # Verificar si hubo algún error en la solicitud
        response.raise_for_status()

        logging.info(f"Archivo subido correctamente a GeoNetwork. Respuesta: {response.text}")
    except Exception as e:
        if response is not None:
            logging.error(f"Código de estado: {response.status_code}, Respuesta: {response.text}")

        logging.error(f"Error al subir el archivo a GeoNetwork: {e}")
        raise Exception(f"Error al subir el archivo a GeoNetwork: {e}")

# Función para generar el XML dinámicamente desde el JSON
def generate_xml(**kwargs):
    try:
        # Obtener los datos del JSON
        algorithm_result = kwargs['dag_run'].conf.get('json')
        
        # Crear la estructura base del XML usando ElementTree
        gmd = "http://www.isotc211.org/2005/gmd"
        gco = "http://www.isotc211.org/2005/gco"
        gmx = "http://www.isotc211.org/2005/gmx"
        ET.register_namespace("gmd", gmd)
        ET.register_namespace("gco", gco)
        ET.register_namespace("gmx", gmx)

        root = ET.Element(f"{{{gmd}}}MD_Metadata", {
            'xmlns:gmd': gmd,
            'xmlns:gco': gco,
            'xmlns:gmx': gmx,
            'xsi:schemaLocation': "http://www.isotc211.org/2005/gmd http://schemas.opengis.net/csw/2.0.2/profiles/apiso/1.0.0/apiso.xsd"
        })

        # Datos dinámicos que se extraen del JSON
        identifier = algorithm_result['executionResources'][1]['data'][0]['value']
        date_stamp = algorithm_result['metadata'][3]['value']
        pixel_size = algorithm_result['executionResources'][1]['data'][3]['value']
        spatial_representation_type = algorithm_result['executionResources'][1]['data'][4]['value']

        # Datos estáticos
        organisation_name = "Instituto geográfico nacional (IGN)"
        email = "ignis@organizacion.es"
        title_value = "Ortomosaico_0026_4740004_611271"
        west_long = "-7.6392"
        east_long = "-7.6336"
        south_lat = "42.8025"
        north_lat = "42.8044"

        # Elemento fileIdentifier
        file_identifier = ET.SubElement(root, f"{{{gmd}}}fileIdentifier")
        character_string = ET.SubElement(file_identifier, f"{{{gco}}}CharacterString")
        character_string.text = identifier

        # Elemento language (estático)
        language = ET.SubElement(root, f"{{{gmd}}}language")
        language_code = ET.SubElement(language, f"{{{gmd}}}LanguageCode", {
            'codeList': "http://www.loc.gov/standards/iso639-2/",
            'codeListValue': "spa"
        })

        # Elemento contacto (estático)
        contact = ET.SubElement(root, f"{{{gmd}}}contact")
        responsible_party = ET.SubElement(contact, f"{{{gmd}}}CI_ResponsibleParty")
        organisation = ET.SubElement(responsible_party, f"{{{gco}}}CharacterString")
        organisation.text = organisation_name
        email_elem = ET.SubElement(responsible_party, f"{{{gmd}}}electronicMailAddress")
        email_char = ET.SubElement(email_elem, f"{{{gco}}}CharacterString")
        email_char.text = email

        # Elemento dateStamp (dinámico del JSON)
        date_stamp_elem = ET.SubElement(root, f"{{{gmd}}}dateStamp")
        date_time = ET.SubElement(date_stamp_elem, f"{{{gco}}}DateTime")
        date_time.text = date_stamp

        # Agregar extent (coordenadas)
        extent = ET.SubElement(root, f"{{{gmd}}}extent")
        geographic_element = ET.SubElement(extent, f"{{{gmd}}}geographicElement")
        bbox = ET.SubElement(geographic_element, f"{{{gmd}}}EX_GeographicBoundingBox")

        west_bound_long = ET.SubElement(bbox, f"{{{gmd}}}westBoundLongitude")
        west_bound_long.text = west_long

        east_bound_long = ET.SubElement(bbox, f"{{{gmd}}}eastBoundLongitude")
        east_bound_long.text = east_long

        south_bound_lat = ET.SubElement(bbox, f"{{{gmd}}}southBoundLatitude")
        south_bound_lat.text = south_lat

        north_bound_lat = ET.SubElement(bbox, f"{{{gmd}}}northBoundLatitude")
        north_bound_lat.text = north_lat

        # Otros elementos del XML se añadirían de forma similar...

        # Generar el XML en string
        xml_string = ET.tostring(root, encoding='utf-8', method='xml')
        logging.info(f"XML generado: {xml_string}")

        # Codificar a base64
        xml_base64 = base64.b64encode(xml_string).decode('utf-8')

        # Retornar el XML en base64 para ser usado por el siguiente paso
        return xml_base64

    except Exception as e:
        logging.error(f"Error generando el XML: {e}")
        raise

# Definición del DAG de Airflow
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 1),
    'retries': 0
}

dag = DAG('generate_and_upload_xml', default_args=default_args, schedule_interval=None)

# Tarea para generar el XML
generate_xml_task = PythonOperator(
    task_id='generate_xml',
    python_callable=generate_xml,
    provide_context=True,
    dag=dag
)

# Tarea para subir el XML
upload_xml_task = PythonOperator(
    task_id='upload_xml',
    python_callable=upload_to_geonetwork,
    provide_context=True,
    dag=dag
)

# Definir la secuencia de tareas
generate_xml_task >> upload_xml_task
