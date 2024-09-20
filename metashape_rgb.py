from collections import defaultdict
from datetime import datetime, timedelta
import re
from airflow import DAG
import requests
from requests.auth import HTTPBasicAuth
from airflow.operators.python import PythonOperator

# Función para crear una nueva sesión de importación en GeoServer para el workspace dado
def create_import_session(workspace, geoserver_url, geoserver_user, geoserver_password):
    import_url = f"{geoserver_url}/rest/imports"
    headers = {
        'Content-type': 'application/json'
    }
    data = {
        "import": {
            "targetWorkspace": {
                "workspace": {
                    "name": workspace
                }
            }
        }
    }
    try:
        response = requests.post(import_url, headers=headers, json=data, auth=HTTPBasicAuth(geoserver_user, geoserver_password))
        if response.status_code == 201:
            import_id = response.json()["import"]["id"]
            print(f"Sesión de importación {import_id} creada con éxito.")
            return import_id
        else:
            print(f"Error al crear la sesión de importación: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Error creando la sesión de importación: {str(e)}")
        return None

# Función para subir el archivo TIFF a la sesión de importación
def upload_file_to_import(import_id, tif_file, geoserver_url, geoserver_user, geoserver_password):
    upload_url = f"{geoserver_url}/rest/imports/{import_id}/tasks"
    files = {
        'file': (tif_file['file_name'], tif_file['content'], 'image/tiff')
    }

    try:
        response = requests.post(upload_url, files=files, auth=HTTPBasicAuth(geoserver_user, geoserver_password))
        if response.status_code == 201:
            print(f"Archivo {tif_file['file_name']} subido con éxito.")
            return True
        else:
            print(f"Error al subir el archivo {tif_file['file_name']}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"Error subiendo el archivo a la sesión de importación: {str(e)}")
        return False

# Función para ejecutar la sesión de importación y publicar las capas
def execute_import_session(import_id, geoserver_url, geoserver_user, geoserver_password):
    execute_url = f"{geoserver_url}/rest/imports/{import_id}?execute=true"
    
    try:
        response = requests.post(execute_url, auth=HTTPBasicAuth(geoserver_user, geoserver_password))
        if response.status_code == 204:
            print(f"Sesión de importación {import_id} ejecutada con éxito.")
            return True
        else:
            print(f"Error al ejecutar la sesión de importación: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"Error ejecutando la sesión de importación: {str(e)}")
        return False

# Primer paso: leer y procesar los archivos extraídos
def process_extracted_files(**kwargs):
    otros = kwargs['dag_run'].conf.get('otros', [])
    json_content = kwargs['dag_run'].conf.get('json')

    if not json_content:
        print("Error con el traspaso de los documentos")
        return

    print("Archivos para procesar preparados")

    # Agrupar los archivos 'otros' por carpetas utilizando regex para extraer el prefijo de la carpeta
    grouped_files = defaultdict(list)
    for file_info in otros:
        file_name = file_info['file_name']

        # Verificar si el archivo es un .tif
        match = re.match(r'.+\.tif$', file_name)
        if match:
            grouped_files['tif_files'].append(file_info)

    # Verificar si se han leído los archivos TIFF
    tif_files = grouped_files.get('tif_files', [])
    if len(tif_files) > 0:
        print(f"Se han leído {len(tif_files)} archivos TIFF correctamente.")
    else:
        print("Error: No se encontraron archivos TIFF.")

    # Guardar la lista de archivos TIFF procesados en XCom para la siguiente tarea
    ti = kwargs['ti']
    ti.xcom_push(key='tif_files', value=tif_files)

# Segundo paso: crear la sesión de importación, subir los archivos y ejecutarla
def upload_files_to_geoserver(**kwargs):
    ti = kwargs['ti']
    tif_files = ti.xcom_pull(key='tif_files', task_ids='process_extracted_files_task')

    if not tif_files:
        print("No se encontraron archivos TIFF para subir.")
        return

    workspace = "metashapergb"  # Cambia esto por el nombre correcto de tu workspace
    geoserver_url = "http://vps-52d8b534.vps.ovh.net:8084/geoserver/rest/workspaces/metashapergb"  # URL de tu servidor GeoServer
    geoserver_user = "admin"  # Usuario de GeoServer
    geoserver_password = "geoserver"  # Contraseña de GeoServer

    # Crear una sesión de importación
    import_id = create_import_session(workspace, geoserver_url, geoserver_user, geoserver_password)
    if not import_id:
        return

    # Subir todos los archivos TIFF
    for tif_file in tif_files:
        if not upload_file_to_import(import_id, tif_file, geoserver_url, geoserver_user, geoserver_password):
            print(f"Error al subir {tif_file['file_name']}. Deteniendo el proceso.")
            return

    # Ejecutar la sesión de importación para publicar las capas
    if not execute_import_session(import_id, geoserver_url, geoserver_user, geoserver_password):
        print(f"Error al ejecutar la sesión de importación {import_id}.")
        return

    print(f"Todos los archivos TIFF se han subido y procesado correctamente en la sesión de importación {import_id}.")

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


# Tarea 1: Procesar los archivos extraídos
process_extracted_files_task = PythonOperator(
    task_id='process_extracted_files_task',
    python_callable=process_extracted_files,
    provide_context=True,
    dag=dag,
)

# Tarea 2: Subir los archivos a GeoServer y ejecutar la sesión de importación
upload_files_to_geoserver_task = PythonOperator(
    task_id='upload_files_to_geoserver_task',
    python_callable=upload_files_to_geoserver,
    provide_context=True,
    dag=dag,
)

# Definir la secuencia de las tareas
process_extracted_files_task >> upload_files_to_geoserver_task
