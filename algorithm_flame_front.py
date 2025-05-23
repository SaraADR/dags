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
from PIL import Image

def process_json(**kwargs):
    otros = kwargs['dag_run'].conf.get('otros', [])
    json_content_original = kwargs['dag_run'].conf.get('json')

    if not json_content_original:
        print("Ha habido un error con el traspaso de los documentos")
        return
    print("Archivos para procesar preparados")

    s3_client = get_minio_client()
    if isinstance(otros, list):
        file_path_in_minio = otros[0].replace('tmp/', '')
    elif isinstance(otros, str):
        file_path_in_minio = otros.replace('tmp/', '')
    else:
        raise ValueError(f"Formato no válido para 'otros': {otros}")

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

            updated_json = generateSimplifiedCopy(json_content_original)

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

            # Procesar thumbnail si existe
            if 'thumbnail' in updated_json:
                try:
                    thumbnail_bytes = base64.b64decode(updated_json['thumbnail'])
                    image = Image.open(io.BytesIO(thumbnail_bytes)).convert('RGB')
                    thumbnail_buffer = io.BytesIO()
                    image.save(thumbnail_buffer, format='PNG')
                    thumbnail_buffer.seek(0)
                    thumbnail_key = f"{id_mission}/{uuid_key}/resources/thumbnail.png"
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=thumbnail_key,
                        Body=thumbnail_buffer,
                        ContentType='image/png'
                    )
                    print(f"Thumbnail convertido y subido a MinIO: {thumbnail_key}")
                except Exception as e:
                    print(f"Error al procesar el thumbnail: {e}")
                del updated_json['thumbnail']

            # Subir el JSON original
            s3_client.put_object(
                Bucket=bucket_name,
                Key=json_key,
                Body=io.BytesIO(json.dumps(json_content_original).encode('utf-8')),
                ContentType='application/json'
            )
            print(f'Archivo JSON original subido correctamente a MinIO como {json_key}')

            # Crear índices de archivos y carpetas extraídas
            file_lookup = {}
            dir_lookup = {}

            for root, dirs, files in os.walk(temp_dir):
                for dir_name in dirs:
                    local_dir_path = os.path.join(root, dir_name)
                    relative_dir_path = os.path.relpath(local_dir_path, temp_dir)
                    parts = relative_dir_path.split(os.sep)
                    if parts[0].lower().startswith("flame"):
                        relative_dir_path = os.path.join(*parts[1:]) if len(parts) > 1 else parts[0]
                    dir_lookup[dir_name] = relative_dir_path

                for file_name in files:
                    if file_name.lower() == 'algorithm_result.json':
                        continue
                    local_path = os.path.join(root, file_name)
                    relative_path = os.path.relpath(local_path, temp_dir)
                    parts = relative_path.split(os.sep)
                    if parts[0].lower().startswith("flame"):
                        relative_path = os.path.join(*parts[1:]) if len(parts) > 1 else parts[0]
                    file_lookup[file_name] = relative_path

            # Actualizar rutas en el JSON usando el índice real
            for resource in updated_json.get("executionResources", []):
                old_path = resource['path']
                file_name = os.path.basename(old_path)
                relative_path = file_lookup.get(file_name)

                if not relative_path:
                    relative_path = dir_lookup.get(file_name)

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
                    parts = relative_path.split(os.sep)
                    if parts[0].lower().startswith("flame"):
                        relative_path = os.path.join(*parts[1:]) if len(parts) > 1 else parts[0]
                    minio_key = f"{id_mission}/{uuid_key}/{relative_path}"
                    try:
                        with open(local_path, 'rb') as file_data:
                            content_type = 'image/tiff' if relative_path.endswith('.tif') else 'application/octet-stream'
                            s3_client.put_object(
                                Bucket=bucket_name,
                                Key=minio_key,
                                Body=file_data,
                                ContentType=content_type
                            )
                            print(f"Archivo subido a MinIO: {minio_key}")
                    except Exception as e:
                        print(f"Error al subir {file_name} a MinIO: {e}")

            # Historizar
            historizacion(json_content_original, updated_json, id_mission, startTimeStamp, endTimeStamp)

def generateSimplifiedCopy(json_content_original):
    updated_json = json.loads(json.dumps(json_content_original))
    keys_a_mantener = {'identifier', 'pixelSize'}
    updated_json.pop("executionArguments", None)
    for resource in updated_json.get("executionResources", []):
        filtered_data = [item for item in resource.get("data", []) if item["name"] in keys_a_mantener]
        resource["data"] = filtered_data
    updated_json["executionResources"] = [
        res for res in updated_json["executionResources"] if res.get("output", False)
    ]
    return updated_json

def historizacion(input_data, output_data, mission_id, startTimeStamp, endTimeStamp):
    try:
        print("Guardamos en historización/flamefront")
        madrid_tz = pytz.timezone('Europe/Madrid')
        start_dt = datetime.strptime(startTimeStamp, "%Y%m%dT%H%M%S")
        end_dt = datetime.strptime(endTimeStamp, "%Y%m%dT%H%M%S")
        phenomenon_time = f"[{start_dt.strftime('%Y-%m-%dT%H:%M:%S')}, {end_dt.strftime('%Y-%m-%dT%H:%M:%S')}]"

        datos = {
            'sampled_feature': mission_id,
            'result_time': datetime.now(madrid_tz),
            'phenomenon_time': phenomenon_time,
            'input_data': json.dumps(input_data),
            'output_data': json.dumps(output_data)
        }

        query = f"""
            INSERT INTO algoritmos.algorithm_flamefront (
                sampled_feature, result_time, phenomenon_time, input_data, output_data
            ) VALUES (
                {datos['sampled_feature']},
                '{datos['result_time']}',
                '{datos['phenomenon_time']}'::TSRANGE,
                '{datos['input_data']}',
                '{datos['output_data']}'
            )
        """
        execute_query('biobd', query)
    except Exception as e:
        print(f"Error en el proceso: {e}")

# Definición del DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'retries': 1,
}

dag = DAG(
    'algorithm_flame_front',
    default_args=default_args,
    description='DAG para analizar flame front con thumbnail',
    schedule_interval=None,
    catchup=False
)

process_task = PythonOperator(
    task_id='process_task_flame_front',
    python_callable=process_json,
    provide_context=True,
    dag=dag
)

process_task
