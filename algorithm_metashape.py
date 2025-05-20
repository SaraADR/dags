import base64
import os
import tempfile
import zipfile

import pytz
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from dag_utils import get_minio_client, execute_query
from airflow.models import Variable
from kafka_consumer_classify_files_and_trigger_dags import download_from_minio
import uuid
import io
import json


def process_json(**kwargs):

    otros = kwargs['dag_run'].conf.get('otros', [])
    json_content_original = kwargs['dag_run'].conf.get('json')

    trace_id = kwargs['dag_run'].conf['trace_id']
    print(f"Processing with trace_id: {trace_id}")

    if not json_content_original:
        print("Ha habido un error con el traspaso de los documentos")
        return
    print("Archivos para procesar preparados")

    s3_client = get_minio_client()
    archivos = []
    file_path_in_minio =  otros.replace('tmp/','')
    folder_prefix = 'sftp/'
    try:
        local_zip_path = download_from_minio(s3_client, 'tmp', file_path_in_minio, 'tmp', folder_prefix)
        print(local_zip_path)
    except Exception as e:
        print(f"Error al descargar desde MinIO: {e}")
        raise

    if local_zip_path is None:
        print(f"No se pudo descargar el archivo desde MinIO: {local_zip_path}")
        return

    try:
        if not os.path.exists(local_zip_path):
            print(f"Archivo no encontrado: {local_zip_path}")
            return


        # Abre y procesa el archivo ZIP desde el sistema de archivos
        with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
            zip_file.testzip()
            print("El archivo ZIP es válido.")
    except zipfile.BadZipFile:
        print("El archivo no es un ZIP válido antes del procesamiento.")
        return

    try:
        with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
            with tempfile.TemporaryDirectory() as temp_dir:
                print(f"Directorio temporal creado: {temp_dir}")
                zip_file.extractall(temp_dir)
                file_list = zip_file.namelist()
                print("Archivos en el ZIP:", file_list)
                archivos = []
                folder_structure = {}
                for file_name in file_list:
                    file_path = os.path.join(temp_dir, file_name)

                    if os.path.isdir(file_path):
                        # Si es un directorio, saltamos
                        continue

                    print(f"Procesando archivo: {file_name}")
                    with open(file_path, 'rb') as f:
                        content = f.read()

                    directory = os.path.dirname(file_name)
                    if directory not in folder_structure:
                        folder_structure[directory] = []
                    folder_structure[directory].append(file_name)
                    if os.path.basename(file_name).lower() == 'algorithm_result.json':
                        continue
                    else:
                        if file_name.lower().endswith('.las') and "cloud-" in file_name:
                            empty_content = b''
                            encoded_empty_content = base64.b64encode(empty_content).decode('utf-8')
                            archivos.append({'file_name': file_name, 'content': encoded_empty_content})
                            print(f"Archivo .las padre {file_name} procesado con 0 bytes")
                        else:
                            encoded_content = base64.b64encode(content).decode('utf-8')
                            archivos.append({'file_name': file_name, 'content': encoded_content})
                print("Estructura de carpetas y archivos en el ZIP:", folder_structure)

    except zipfile.BadZipFile as e:
        print(f"El archivo no es un ZIP válido: {e}")
        return

    # json_content_original = kwargs['dag_run'].conf.get('json')

    # Clonamos el JSON para no modificar el original
    updated_json = json.loads(json.dumps(json_content_original))  # deep copy

    # Obtener MissionID
    id_mission = next(
        (item['value'] for item in updated_json['metadata'] if item['name'] == 'MissionID'),
        None
    )
    print("ID de mision encontrado:", id_mission)

    startTimeStamp = updated_json['startTimestamp']
    endTimeStamp = updated_json['endTimestamp']
    print("startTimeStamp:", startTimeStamp)
    print("endTimeStamp", endTimeStamp)

    s3_client = get_minio_client()
    uuid_key = str(uuid.uuid4())
    bucket_name = 'missions'
    json_key = f"{id_mission}/{uuid_key}/algorithm_result.json"

    # Actualizamos solo en el JSON nuevo
    for resource in updated_json.get("executionResources", []):
        old_path = resource['path']
        filename = old_path.split("/")[-1]
        new_path = f"/{bucket_name}/{id_mission}/{uuid_key}/{filename}"
        resource['path'] = new_path
        print(f"Ruta actualizada: {old_path} -> {new_path}")

    # Subimos el JSON actualizado a MinIO
    s3_client.put_object(
        Bucket=bucket_name,
        Key=json_key,
        Body=io.BytesIO(json.dumps(updated_json).encode('utf-8')),
        ContentType='application/json'
    )
    print(f'Archivo JSON actualizado subido correctamente a MinIO como {json_key}')

    #Metodo para la historización
    historizacion(json_content_original, updated_json, id_mission, startTimeStamp, endTimeStamp)


#HISTORIZACION
def historizacion(input_data, output_data, mission_id, startTimeStamp, endTimeStamp):
    try:
        # Y guardamos en la tabla de historico
            print("Guardamos en historización/metashape")
            madrid_tz = pytz.timezone('Europe/Madrid')

            # Formatear phenomenon_time
            start_dt = datetime.strptime(startTimeStamp, "%Y%m%dT%H%M%S")
            end_dt = datetime.strptime(endTimeStamp, "%Y%m%dT%H%M%S")

            phenomenon_time = f"[{start_dt.strftime('%Y-%m-%dT%H:%M:%S')}, {end_dt.strftime('%Y-%m-%dT%H:%M:%S')}]"
            datos = {
                'sampled_feature': mission_id, 
                'result_time': datetime.now(madrid_tz),
                'phenomenon_time': phenomenon_time,
                'input_data': json.dumps(input_data), #rutas antiguas
                'output_data': json.dumps(output_data) #JSON MODIFICADOOO CON LO NUEVO
                # se crea un json apartir del antiguo con las rutas nuevas en minio
            }

            # Construir la consulta de inserción
            query = f"""
                INSERT INTO algoritmos.algoritmo_water_analysis (
                    sampled_feature, result_time, phenomenon_time, input_data, output_data
                ) VALUES (
                    {datos['sampled_feature']},
                    '{datos['result_time']}',
                    '{datos['phenomenon_time']}'::TSRANGE,
                    '{datos['input_data']}',
                    '{datos['output_data']}'
                )
            """

            # Ejecutar la consulta
            execute_query('biobd', query)
    except Exception as e:
        print(f"Error en el proceso: {str(e)}")



# Definición del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'retries': 1,
}

dag = DAG(
    'algorithm_metashape',
    default_args=default_args,
    description='DAG para analizar metashape',
    schedule_interval=None,  # Se puede ajustar según necesidades
    catchup=False
)

process_task = PythonOperator(
    task_id='process_task_metashape',
    python_callable=process_json, #Importante
    provide_context=True,
    dag=dag
)

# Definir el flujo de las tareas
process_task