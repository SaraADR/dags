from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.http_hook import HttpHook
from airflow.utils.dates import days_ago
import os

# Argumentos predeterminados del DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Ruta del archivo XML generado (asegúrate de que sea correcto)
xml_file_path = "recursos/archivo.xml"  # Cambia esto por la ruta correcta de tu archivo XML generado

# Función para ejecutar el script que genera el XML
def generate_xml():
    # Ejecuta tu script para generar el XML
    os.system('python /mnt/data/metashape_rgb.py')

# Función para subir el archivo XML a GeoNetwork
def upload_to_geonetwork():
    # Inicializa el hook HTTP con la URL de GeoNetwork
    http_hook = HttpHook(http_conn_id='geonetwork_conn', method='POST')
    
    # URL para la subida del archivo
    upload_url = "https://eiiob.dev.cuatrodigital.com/geonetwork/srv/api/0.1/records"

    # Verifica si el archivo XML existe
    if not os.path.exists(xml_file_path):
        raise FileNotFoundError(f"El archivo XML no fue encontrado en la ruta: {xml_file_path}")
    
    # Lee el contenido del archivo XML
    with open(xml_file_path, 'rb') as f:
        xml_data = f.read()

    # Configura los headers para la solicitud
    headers = {
        'Content-Type': 'application/xml'
    }
    
    # Credenciales de autenticación
    auth = {
        "username": "angel",
        "password": "111111"
    }

    # Ejecuta la solicitud POST para subir el archivo XML
    response = http_hook.run(endpoint=upload_url, data=xml_data, headers=headers, extra_options={'auth': auth})
    
    # Maneja la respuesta
    if response.status_code == 201:
        print("Archivo XML subido correctamente.")
    else:
        print(f"Error en la subida: {response.text}")

# Definir el DAG
with DAG(
   'metashape_rgb',
     default_args=default_args,
     description='Flujo de datos de entrada de elementos de metashape_rgb',
     schedule_interval=None,
    catchup=False,
) as dag:
    
    # Tarea para generar el archivo XML
    generate_xml_task = PythonOperator(
        task_id='generate_xml',
        python_callable=generate_xml
    )
    
    # Tarea para subir el archivo XML a GeoNetwork
    upload_xml_task = PythonOperator(
        task_id='upload_xml',
        python_callable=upload_to_geonetwork
    )

    # Definir el flujo de tareas
    generate_xml_task >> upload_xml_task
