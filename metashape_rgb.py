import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import re
import json
from collections import defaultdict
from airflow.operators.python import PythonOperator
from airflow import DAG

# Parámetros de conexión a GeoServer
GEOSERVER_URL = "https://geoserver.dev.cuatrodigital.com/geoserver/web/"
GEOSERVER_USER = "admin"
GEOSERVER_PASSWORD = "geoserver"
WORKSPACE = "tests-geonetwork"
DATASTORE = "GeoTiffs Metashapes"

def upload_to_geoserver(tif_file, file_name):
    """
    Función para subir un archivo TIFF a GeoServer usando la API REST.
    """
    url = f"{GEOSERVER_URL}/rest/workspaces/{WORKSPACE}/coveragestores/{file_name}/file.geotiff"
    
    headers = {
        'Content-Type': 'image/tiff',
    }
    
    response = requests.put(
        url,
        headers=headers,
        data=tif_file['content'],
        auth=HTTPBasicAuth(GEOSERVER_USER, GEOSERVER_PASSWORD)
    )
    
    if response.status_code == 201:
        print(f"El archivo {file_name} se ha subido correctamente a GeoServer.")
        return True
    else:
        print(f"Error al subir {file_name} a GeoServer. Código de estado: {response.status_code}")
        print(response.text)
        return False

def create_wms_layer(file_name):
    """
    Función para crear una capa WMS en GeoServer a partir de un archivo subido.
    """
    url = f"{GEOSERVER_URL}/rest/workspaces/{WORKSPACE}/coveragestores/{file_name}/coverages"
    
    data = {
        "coverage": {
            "name": file_name,
            "nativeName": file_name,
            "namespace": {"name": WORKSPACE},
            "store": {"name": file_name, "workspace": WORKSPACE},
        }
    }
    
    headers = {
        'Content-Type': 'application/json',
    }
    
    response = requests.post(
        url,
        headers=headers,
        data=json.dumps(data),
        auth=HTTPBasicAuth(GEOSERVER_USER, GEOSERVER_PASSWORD)
    )
    
    if response.status_code == 201:
        print(f"La capa WMS para {file_name} se ha creado correctamente.")
        return f"{GEOSERVER_URL}/{WORKSPACE}/wms?service=WMS&version=1.1.0&request=GetMap&layers={WORKSPACE}:{file_name}&bbox=-180,-90,180,90&width=768&height=330&srs=EPSG:4326&format=application/openlayers"
    else:
        print(f"Error al crear la capa WMS para {file_name}. Código de estado: {response.status_code}")
        print(response.text)
        return None

def process_extracted_files(**kwargs):
    # Obtenemos los archivos 
    otros = kwargs['dag_run'].conf.get('otros', [])
    json_content = kwargs['dag_run'].conf.get('json')

    if not json_content:
        print("Ha habido un error con el traspaso de los documentos")
        return

    print("Archivos para procesar preparados")

    # Agrupamos 'otros' por carpetas utilizando regex para extraer el prefijo de la carpeta
    grouped_files = defaultdict(list)
    for file_info in otros:
        file_name = file_info['file_name']

        # Verificar si el archivo es un .tif
        match = re.match(r'.+\.tif$', file_name)
        
        if match:
            grouped_files['tif_files'].append(file_info)

    # Verificar si se han leído los 3 archivos .tif
    tif_files = grouped_files.get('tif_files', [])
    if len(tif_files) == 3:
        print("Se han leído los 3 archivos TIFF correctamente.")
    else:
        print(f"Error: Se esperaban 3 archivos TIFF, pero se encontraron {len(tif_files)}.")
        return

    # Mostrar los nombres de los archivos .tif leídos y subirlos a GeoServer
    wms_urls = []
    for tif_file in tif_files:
        file_name = tif_file['file_name'].split('/')[-1].replace('.tif', '')
        print(f"Subiendo archivo TIFF: {tif_file['file_name']}")
        
        # Subir el archivo a GeoServer
        upload_success = upload_to_geoserver(tif_file, file_name)
        
        if upload_success:
            # Crear la capa WMS
            wms_url = create_wms_layer(file_name)
            if wms_url:
                wms_urls.append(wms_url)
    
    # Mostrar las URLs WMS creadas
    for url in wms_urls:
        print(f"Capa WMS creada: {url}")
    return wms_urls

# Ejemplo de uso en DAG de Airflow
default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'metashape_rgb',
    default_args=default_args,
    description='Flujo de datos de entrada de elementos de metashape_rgb',
    schedule_interval=None,
    catchup=False,
)

process_extracted_files_task = PythonOperator(
    task_id='process_extracted_files_task',
    python_callable=process_extracted_files,
    provide_context=True,
    dag=dag,
)

process_extracted_files_task
