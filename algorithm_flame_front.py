import json
import os

import pytz
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from dag_utils import get_minio_client, execute_query
from airflow.models import Variable
import uuid
import io


def process_json(**kwargs):
    json_content_original = kwargs['dag_run'].conf.get('json')
    file_list = kwargs['dag_run'].conf.get('file_list', [])  # rutas relativas desde 'resources/'

    if not json_content_original or not file_list:
        print("Faltan datos en la configuración del DAG: JSON o lista de archivos.")
        return

    print("Archivos a procesar directamente:", file_list)

    updated_json = json.loads(json.dumps(json_content_original))

    id_mission = next(
        (item['value'] for item in updated_json['metadata'] if item['name'] == 'MissionID'),
        None
    )
    print("ID de misión:", id_mission)

    startTimeStamp = updated_json['startTimestamp']
    endTimeStamp = updated_json['endTimestamp']

    s3_client = get_minio_client()
    bucket_name = 'missions'
    uuid_key = str(uuid.uuid4())
    json_key = f"{id_mission}/{uuid_key}/algorithm_result.json"
    ruta_minio = Variable.get("ruta_minIO")

    # Actualizar paths en el JSON
    for resource in updated_json.get("executionResources", []):
        old_path = resource['path']
        file_name = os.path.basename(old_path)

        relative_path = next((f for f in file_list if f.endswith(file_name)), None)
        if not relative_path:
            print(f"No se encontró {file_name} en la lista de archivos proporcionada.")
            continue

        new_path = f"/{bucket_name}/{id_mission}/{uuid_key}/{relative_path}"
        full_path = f"{ruta_minio.rstrip('/')}{new_path}"
        resource['path'] = full_path
        print(f"Ruta actualizada: {old_path} -> {full_path}")

    # Subir el JSON modificado a MinIO
    s3_client.put_object(
        Bucket=bucket_name,
        Key=json_key,
        Body=io.BytesIO(json.dumps(updated_json).encode('utf-8')),
        ContentType='application/json'
    )
    print(f"JSON actualizado subido a MinIO como {json_key}")

    historizacion(json_content_original, updated_json, id_mission, startTimeStamp, endTimeStamp)


# HISTORIZACION
def historizacion(input_data, output_data, mission_id, startTimeStamp, endTimeStamp):
    try:
        # Y guardamos en la tabla de historico
        print("Guardamos en historización/flamefront")
        madrid_tz = pytz.timezone('Europe/Madrid')

        # Formatear phenomenon_time
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

        # Construir la consulta de inserción
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
    'algorithm_flame_front',
    default_args=default_args,
    description='DAG para analizar frente de llamas',
    schedule_interval=None,
    catchup=False
)

process_task = PythonOperator(
    task_id='process_task_flame_front',
    python_callable=process_json,
    provide_context=True,
    dag=dag
)

# Definir el flujo de las tareas
process_task