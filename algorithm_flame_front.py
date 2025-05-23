import json
import os
import base64
import io
import uuid
import mimetypes
import pytz
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from dag_utils import get_minio_client, execute_query


def process_json(**kwargs):
    # ----- Lectura de inputs -----
    json_in  = kwargs['dag_run'].conf.get('json')
    file_list = kwargs['ti'].xcom_pull(key='file_list', task_ids='list_files')
    if not json_in or not file_list:
        print("Faltan JSON o file_list.")
        return

    updated = json.loads(json.dumps(json_in))

    id_mission = next((m['value'] for m in updated['metadata'] if m['name']=='MissionID'), None)
    start_ts   = updated['startTimestamp']
    end_ts     = updated['endTimestamp']

    # Cliente MinIO y vars
    s3      = get_minio_client()
    bucket  = 'missions'
    uuidkey = str(uuid.uuid4())
    baseurl = Variable.get("ruta_minIO").rstrip('/')

    # ---- Procesar cada recurso ----
    for resource in updated.get("executionResources", []):
        old_path = resource.get('path','')
        fname    = os.path.basename(old_path)

        # 1) localiza en file_list
        rel = next((f for f in file_list if f.endswith(fname)), None)
        if not rel:
            print(f"No hallé {fname} en file_list.")
            continue

        # 2) se sube el TIFF (u otro) original
        local = os.path.join('resources', rel)
        with open(local,'rb') as f: blob = f.read()
        key_orig = f"{id_mission}/{uuidkey}/{rel}"
        ctype = mimetypes.guess_type(local)[0] or 'application/octet-stream'
        s3.put_object(Bucket=bucket, Key=key_orig, Body=blob, ContentType=ctype)
        resource['path'] = f"{baseurl}/{bucket}/{key_orig}"
        print(f"Subido {local} → {bucket}/{key_orig}")

        # 3) busca thumbnail en cada entrada de data[]
        for entry in resource.get('data', []):
            val = entry.get('value', {})
            thumb_b64 = val.get('thumbnail', {}).get('thumbRef')
            if not thumb_b64:
                continue

            # se decodifica y se sube PNG
            img = base64.b64decode(thumb_b64)
            thumb_name = os.path.splitext(fname)[0] + "_thumbnail.png"
            key_thumb  = f"{id_mission}/{uuidkey}/{thumb_name}"
            s3.put_object(
                Bucket=bucket,
                Key=key_thumb,
                Body=io.BytesIO(img),
                ContentType='image/png'
            )
            # actualizar JSON
            val['thumbnail']['path'] = f"{baseurl}/{bucket}/{key_thumb}"
            print(f"Thumbnail subido → {bucket}/{key_thumb}")

    # ---- por último se sube el JSON modificado ----
    key_json = f"{id_mission}/{uuidkey}/algorithm_result.json"
    s3.put_object(
        Bucket=bucket,
        Key=key_json,
        Body=io.BytesIO(json.dumps(updated).encode('utf-8')),
        ContentType='application/json'
    )
    print(f"JSON final subido → {bucket}/{key_json}")

    # historización (tu función igual que antes)
    historizacion(json_in, updated, id_mission, start_ts, end_ts)



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