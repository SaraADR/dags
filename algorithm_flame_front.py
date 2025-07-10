# -*- coding: utf-8 -*-
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
from PIL import Image, UnidentifiedImageError
import geopandas as gpd
from shapely.geometry import Point, LineString


def crear_y_subir_shapefiles(detections, image_name, temp_dir, s3_client, bucket_name, minio_base_path):
    uuid_key = minio_base_path.split('/')[2]
    mission_id = minio_base_path.split('/')[1]
    shp_paths = []

    print(f"[DEBUG] Generando SHP para imagen: {image_name}")

    for tipo, elementos in detections.items():
        if tipo not in ["Hotspot", "Frontflame"]:
            continue

        geometries = []
        for item in elementos:
            coords = item.get("world_coordinates", [])
            if tipo == "Hotspot" and coords:
                geometries.append(Point(coords))
            elif tipo == "Frontflame" and coords:
                geometries.append(LineString(coords))

        if not geometries:
            continue

        gdf = gpd.GeoDataFrame(geometry=geometries, crs="EPSG:4326")

        tipo_lower = tipo.lower()
        base_filename = f"{tipo_lower}_{os.path.splitext(image_name)[0]}"
        local_shp_dir = os.path.join(temp_dir, base_filename)
        os.makedirs(local_shp_dir, exist_ok=True)

        local_shp_path = os.path.join(local_shp_dir, base_filename + ".shp")
        gdf.to_file(local_shp_path)

        for ext in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
            file_path = os.path.join(local_shp_dir, base_filename + ext)
            if os.path.exists(file_path):
                minio_key = f"{mission_id}/{uuid_key}/{base_filename}{ext}"
                with open(file_path, 'rb') as file_data:
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=minio_key,
                        Body=file_data,
                        ContentType='application/octet-stream'
                    )
                full_path = f"{Variable.get('ruta_minIO').rstrip('/')}/{bucket_name}/{minio_key}"
                shp_paths.append(full_path)
                print(f"[DEBUG] SHP subido: {full_path}")

    return shp_paths


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

            for resource in updated_json.get("executionResources", []):
                for data_entry in resource.get("data", []):
                    thumb_ref = data_entry.get('value', {}).get('thumbnail', {}).get('thumbRef')
                    if not thumb_ref:
                        continue

                    try:
                        print("[DEBUG] Procesando thumbnail base64 para subirlo como PNG...")
                        cleaned_base64 = thumb_ref.replace('\n', '').replace('\r', '')
                        img_bytes = base64.b64decode(cleaned_base64)
                        main_file_name = os.path.basename(resource['path'])
                        thumb_filename = os.path.splitext(main_file_name)[0] + "_thumbnail.png"
                        thumb_key = f"{id_mission}/{uuid_key}/{thumb_filename}"

                        s3_client.put_object(
                            Bucket=bucket_name,
                            Key=thumb_key,
                            Body=io.BytesIO(img_bytes),
                            ContentType='image/png'
                        )

                        thumb_path = f"{ruta_minio.rstrip('/')}/{bucket_name}/{thumb_key}"
                        data_entry['value']['thumbnail']['path'] = thumb_path
                        print(f"[DEBUG] Thumbnail subido y actualizado en JSON: {thumb_path}")
                        del data_entry['value']['thumbnail']['thumbRef']
                    except Exception as e:
                        print(f"[ERROR] Error procesando el thumbnail: {e}")

            file_lookup = {}
            for root, _, files in os.walk(temp_dir):
                for file_name in files:
                    if file_name.lower() == 'algorithm_result.json':
                        continue
                    local_path = os.path.join(root, file_name)
                    file_lookup[file_name] = local_path

            for resource in updated_json.get("executionResources", []):
                old_path = resource['path']
                file_name = os.path.basename(old_path)
                if file_name not in file_lookup:
                    print(f"No se encontró {file_name} en el ZIP extraído.")
                    continue
                new_path = f"/{bucket_name}/{id_mission}/{uuid_key}/{file_name}"
                full_path = f"{ruta_minio.rstrip('/')}{new_path}"
                resource['path'] = full_path
                print(f"Ruta actualizada: {old_path} -> {full_path}")

            for file_name, local_path in file_lookup.items():
                minio_key = f"{id_mission}/{uuid_key}/{file_name}"
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

            # CREAR SHAPEFILES
            algorithm_json_path = os.path.join(temp_dir, 'algorithm_result.json')
            if os.path.exists(algorithm_json_path):
                with open(algorithm_json_path, 'r', encoding='utf-8') as f:
                    detection_data = json.load(f)

                detecciones_por_imagen = {}
                for elemento in detection_data.get('detections', []):
                    image_name = elemento.get('image_name')
                    if image_name not in detecciones_por_imagen:
                        detecciones_por_imagen[image_name] = {"Hotspot": [], "Frontflame": []}
                    tipo = elemento.get('type')
                    if tipo in ["Hotspot", "Frontflame"]:
                        detecciones_por_imagen[image_name][tipo].append(elemento)

                for resource in updated_json.get("executionResources", []):
                    image_name = os.path.basename(resource['path'])
                    detecciones = detecciones_por_imagen.get(image_name)
                    if detecciones:
                        rutas_shp = crear_y_subir_shapefiles(detecciones, image_name, temp_dir, s3_client, bucket_name, f"{bucket_name}/{id_mission}/{uuid_key}")
                        for path_shp in rutas_shp:
                            resource.setdefault("extraResources", []).append({"type": "shapefile", "path": path_shp})

            s3_client.put_object(
                Bucket=bucket_name,
                Key=json_key,
                Body=io.BytesIO(json.dumps(updated_json).encode('utf-8')),
                ContentType='application/json'
            )
            print(f'Archivo JSON actualizado subido correctamente a MinIO como {json_key}')
            historizacion(json_content_original, updated_json, id_mission, startTimeStamp, endTimeStamp)


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
        print(f"Error en el proceso: {str(e)}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'retries': 1,
}

dag = DAG(
    'algorithm_flame_front',
    default_args=default_args,
    description='DAG para analizar flamefront',
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