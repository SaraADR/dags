from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
from airflow import DAG
from airflow.operators.python import PythonOperator

# Función para crear un Coverage Store en GeoServer
def create_coverage_store(workspace, store_name, geoserver_url, geoserver_user, geoserver_password):
    coverage_store_url = f"{geoserver_url}/rest/workspaces/{workspace}/coveragestores"
    headers = {
        'Content-type': 'application/json'
    }
    data = {
        "coverageStore": {
            "name": store_name,
            "type": "GeoTIFF",
            "enabled": True,
            "workspace": {
                "name": workspace
            },
            "url": f"file:data/{store_name}.tif"
        }
    }
    response = requests.post(coverage_store_url, json=data, headers=headers, auth=HTTPBasicAuth(geoserver_user, geoserver_password))
    
    if response.status_code in [200, 201]:
        print(f"CoverageStore {store_name} creado exitosamente.")
    else:
        print(f"Error al crear el CoverageStore: {response.status_code} - {response.text}")

# Función para subir el archivo TIFF
def upload_tiff_to_geoserver(workspace, store_name, tiff_file_path, geoserver_url, geoserver_user, geoserver_password):
    headers = {
        'Content-type': 'image/tiff'
    }
    
    # Endpoint para subir el archivo TIFF
    upload_url = f"{geoserver_url}/rest/workspaces/{workspace}/coveragestores/{store_name}/file.geotiff"
    
    # Lee el archivo TIFF
    with open(tiff_file_path, 'rb') as tif_file:
        response = requests.put(upload_url, headers=headers, data=tif_file, auth=HTTPBasicAuth(geoserver_user, geoserver_password))
    
    if response.status_code == 201:
        print(f"Archivo {tiff_file_path} subido exitosamente.")
    else:
        print(f"Error al subir el archivo: {response.status_code} - {response.text}")

# Primer paso: procesar los archivos
def process_tiff_files(**kwargs):
    # Aquí obtendrías los archivos de alguna fuente, por ejemplo desde un XCom o similar
    # En este ejemplo estamos simulando que obtuvimos un archivo TIFF
    tiff_files = kwargs.get('dag_run').conf.get('tiff_files', [])
    
    if not tiff_files:
        print("No se encontraron archivos TIFF para procesar.")
        return
    
    # Filtra los archivos TIFF si es necesario, en este caso asumimos que ya tenemos la lista
    ti = kwargs['ti']
    ti.xcom_push(key='tiff_files', value=tiff_files)
    print(f"Se han procesado {len(tiff_files)} archivos TIFF.")

# Segundo paso: subir los archivos TIFF a GeoServer
def upload_tiff_files_to_geoserver(**kwargs):
    ti = kwargs['ti']
    tiff_files = ti.xcom_pull(key='tiff_files', task_ids='process_tiff_files_task')
    
    if not tiff_files:
        print("No se encontraron archivos TIFF para subir.")
        return

    workspace = "metashapergb"  # Cambia esto por tu workspace
    geoserver_url = "http://vps-52d8b534.vps.ovh.net:8084/geoserver/rest/workspaces/metashapergb"
    geoserver_user = "admin"
    geoserver_password = "geoserver"

    for tiff_file in tiff_files:
        store_name = tiff_file['file_name'].split('.')[0]
        tiff_file_path = tiff_file['file_path']  # Asume que tienes la ruta del archivo

        print(f"Subiendo {tiff_file_path} como {store_name}...")

        # Crear Coverage Store
        create_coverage_store(workspace, store_name, geoserver_url, geoserver_user, geoserver_password)

        # Subir el archivo TIFF
        upload_tiff_to_geoserver(workspace, store_name, tiff_file_path, geoserver_url, geoserver_user, geoserver_password)

# Configuración del DAG en Airflow
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'upload_tiff_to_geoserver',
    default_args=default_args,
    description='DAG para subir archivos TIFF a GeoServer',
    schedule_interval=None,
    catchup=False,
)

# Definición de la primera tarea: procesar los archivos
process_tiff_files_task = PythonOperator(
    task_id='process_tiff_files_task',
    python_callable=process_tiff_files,
    provide_context=True,
    dag=dag,
)

# Definición de la segunda tarea: subir los archivos a GeoServer
upload_tiff_files_to_geoserver_task = PythonOperator(
    task_id='upload_tiff_files_to_geoserver_task',
    python_callable=upload_tiff_files_to_geoserver,
    provide_context=True,
    dag=dag,
)

# Dependencia entre las tareas
process_tiff_files_task >> upload_tiff_files_to_geoserver_task
