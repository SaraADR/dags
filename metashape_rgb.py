from datetime import datetime, timedelta
import re
import json
from collections import defaultdict
from airflow.operators.python import PythonOperator
from airflow import DAG

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

    # Mostrar los nombres de los archivos .tif leídos
    for tif_file in tif_files:
        print(f"Archivo TIFF leído: {tif_file['file_name']}")
        # También puedes mostrar características adicionales del archivo si es necesario
        print(f"  Tamaño del archivo: {len(tif_file['content'])} bytes")

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
