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

    if not json_content_original:
        print("Ha habido un error con el traspaso de los documentos")
        return
    print("Archivos para procesar preparados")

    s3_client = get_minio_client()
    file_path_in_minio = otros.replace('tmp/', '')
    folder_prefix = 'sftp/'

    try:
        local_zip_path = download_from_minio(s3_client, 'tmp', file_path_in_minio, 'tmp', folder_prefix)
        print(f"ZIP descargado: {local_zip_path}")
    except Exception as e:
        print(f"Error al descargar desde MinIO: {e}")
        raise

    if local_zip_path is None or not os.path.exists(local_zip_path):
        print(f"Archivo no encontrado: {local_zip_path}")
        return

    try:
        with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
            zip_file.testzip()
            print("El archivo ZIP es válido.")
    except zipfile.BadZipFile:
        print("El archivo no es un ZIP válido antes del procesamiento.")
        return

    with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
        with tempfile.TemporaryDirectory() as temp_dir:
            print(f"Directorio temporal creado: {temp_dir}")
            zip_file.extractall(temp_dir)
            file_list = zip_file.namelist()
            print("Archivos en el ZIP:", file_list)

            updated_json = json.loads(json.dumps(json_content_original))

            id_mission = next(
                (item['value'] for item in updated_json['metadata'] if item['name'] == 'MissionID'),
                None
            )
            print("ID de misión encontrado:", id_mission)

            startTimeStamp = updated_json['startTimestamp']
            endTimeStamp = updated_json['endTimestamp']
            print("startTimeStamp:", startTimeStamp)
            print("endTimeStamp:", endTimeStamp)

            uuid_key = str(uuid.uuid4())
            bucket_name = 'missions'
            json_key = f"{id_mission}/{uuid_key}/algorithm_result.json"
            print(json_key)
            ruta_minio = Variable.get("ruta_minIO")

            # Crear un índice de archivos extraídos
            file_lookup = {}
            for root, dirs, files in os.walk(temp_dir):
                for file_name in files:
                    if file_name.lower() == 'algorithm_result.json':
                        continue
                    local_path = os.path.join(root, file_name)
                    relative_path = os.path.relpath(local_path, temp_dir)

                    # Eliminar el prefijo de carpeta de envoltorio si lo hay (ej: MetashapeIR_2025...)
                    parts = relative_path.split(os.sep)
                    if parts[0].lower().startswith("metashape"):
                        relative_path = os.path.join(*parts[1:])

                    file_lookup[file_name] = relative_path

            # Actualizar rutas en el JSON usando el índice real
            for resource in updated_json.get("executionResources", []):
                old_path = resource['path']
                file_name = os.path.basename(old_path)
                relative_path = file_lookup.get(file_name)

                if not relative_path:
                    print(f"No se encontró {file_name} en el ZIP extraído.")
                    continue

                new_path = f"/{bucket_name}/{id_mission}/{uuid_key}/{relative_path}"
                full_path = f"{ruta_minio.rstrip('/')}{new_path}"
                resource['path'] = full_path
                print(f"Ruta actualizada: {old_path} -> {full_path}")

            # Subir archivos del ZIP a MinIO
            for root, dirs, files in os.walk(temp_dir):
                for file_name in files:
                    if file_name.lower() == 'algorithm_result.json':
                        continue
                    local_path = os.path.join(root, file_name)
                    relative_path = os.path.relpath(local_path, temp_dir)

                    # Eliminar el prefijo tipo MetashapeIR_.../
                    parts = relative_path.split(os.sep)
                    if parts[0].lower().startswith("metashape"):
                        relative_path = os.path.join(*parts[1:])

                    minio_key = f"{id_mission}/{uuid_key}/{relative_path}"
                    try:
                        with open(local_path, 'rb') as file_data:
                            s3_client.put_object(
                                Bucket=bucket_name,
                                Key=minio_key,
                                Body=file_data,
                                ContentType='application/octet-stream'
                            )
                        print(f"Archivo subido a MinIO: {minio_key}")
                    except Exception as e:
                        print(f"Error al subir {file_name} a MinIO: {e}")

            # Subir el JSON actualizado
            s3_client.put_object(
                Bucket=bucket_name,
                Key=json_key,
                Body=io.BytesIO(json.dumps(updated_json).encode('utf-8')),
                ContentType='application/json'
            )
            print(f'Archivo JSON actualizado subido correctamente a MinIO como {json_key}')

            # Historizar
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
                INSERT INTO algoritmos.algorithm_metashape (
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