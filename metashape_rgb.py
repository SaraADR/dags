from collections import defaultdict
from datetime import datetime, timedelta
import re
from airflow import DAG
import requests
from requests.auth import HTTPBasicAuth
from airflow.operators.python import PythonOperator

# Función para subir el archivo a GeoServer y crear la capa
def upload_to_geoserver(tif_file, datastore_name, workspace, geoserver_url, geoserver_user, geoserver_password):
    headers = {
        'Content-type': 'image/tiff'
    }

    # Cambiar a coverage store para archivos TIFF
    coverage_store_url = f"{geoserver_url}/rest/workspaces/{workspace}/coveragestores/{datastore_name}/file.geotiff"

    try:
        # Subir el archivo TIFF a GeoServer
        response = requests.put(
            coverage_store_url,
            headers=headers,
            data=tif_file['content'],  # El contenido del archivo
            auth=HTTPBasicAuth(geoserver_user, geoserver_password)
        )
        
        if response.status_code == 201:
            print(f"Archivo {tif_file['file_name']} subido exitosamente a GeoServer.")
        else:
            print(f"Error al subir el archivo {tif_file['file_name']}: {response.status_code} - {response.text}")
            return None

        # Devuelve la URL WMS correspondiente a la capa creada
        wms_url = f"{geoserver_url}/geoserver/{workspace}/wms?service=WMS&version=1.1.0&request=GetMap&layers={workspace}:{datastore_name}&styles=&bbox=-180,-90,180,90&width=768&height=330&srs=EPSG:4326&format=application/openlayers"
        return wms_url

    except Exception as e:
        print(f"Error subiendo el archivo a GeoServer: {str(e)}")
        return None

# Primer paso: leer y procesar los archivos
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

    # Guardamos la lista de archivos TIFF procesados en XCom para la siguiente tarea
    ti = kwargs['ti']
    ti.xcom_push(key='tif_files', value=tif_files)

# Segundo paso: subir los archivos a GeoServer
def upload_files_to_geoserver(**kwargs):
    # Recuperar los archivos TIFF procesados de XCom
    ti = kwargs['ti']
    tif_files = ti.xcom_pull(key='tif_files', task_ids='process_extracted_files_task')

    if not tif_files:
        print("No se encontraron archivos TIFF para subir.")
        return

    workspace = "tests-geonetwork"  # Cambia esto por el workspace correcto
    geoserver_url = "http://vps-52d8b534.vps.ovh.net:8084/geoserver"  # URL de tu servidor GeoServer (sin /web/)
    geoserver_user = "admin"  # Usuario de GeoServer
    geoserver_password = "geoserver"  # Contraseña de GeoServer

    wms_urls = []
    for tif_file in tif_files:
        datastore_name = tif_file['file_name'].split('.')[0]  # Extraemos el nombre del datastore del nombre del archivo
        print(f"Subiendo archivo TIFF: {tif_file['file_name']} como datastore {datastore_name}")

        # Llamar a la función para subir el archivo a GeoServer
        wms_url = upload_to_geoserver(tif_file, datastore_name, workspace, geoserver_url, geoserver_user, geoserver_password)

        if wms_url:
            print(f"Archivo subido exitosamente. URL WMS: {wms_url}")
            wms_urls.append(wms_url)
        else:
            print(f"Fallo al subir el archivo {tif_file['file_name']} a GeoServer.")

    return wms_urls

# Configuración del DAG de Airflow
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

# Primera tarea: procesar los archivos
process_extracted_files_task = PythonOperator(
    task_id='process_extracted_files_task',
    python_callable=process_extracted_files,
    provide_context=True,
    dag=dag,
)

# Segunda tarea: subir los archivos a GeoServer
upload_files_to_geoserver_task = PythonOperator(
    task_id='upload_files_to_geoserver_task',
    python_callable=upload_files_to_geoserver,
    provide_context=True,
    dag=dag,
)

# Definir la secuencia de las tareas
process_extracted_files_task >> upload_files_to_geoserver_task
