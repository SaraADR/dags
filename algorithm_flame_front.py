import json
import os
import base64
import io
import uuid
import pytz
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from dag_utils import get_minio_client, execute_query


def process_json(**kwargs):
    # ----- Leer inputs -----
    json_in   = kwargs['dag_run'].conf.get('json')
    archivos  = json_in.get('archivos', [])
    if not json_in:
        print("Falta JSON en la configuración del DAG.")
        return

    updated     = json.loads(json.dumps(json_in))
    id_mission  = next((m['value'] for m in updated['metadata'] if m['name']=='MissionID'), None)
    start_ts    = updated['startTimestamp']
    end_ts      = updated['endTimestamp']

    # Cliente MinIO y vars
    s3          = get_minio_client()
    bucket      = 'missions'
    uuidkey     = str(uuid.uuid4())
    baseurl     = Variable.get("ruta_minIO").rstrip('/')
    nuevos_paths = {}

    print(f"MissionID: {id_mission}")
    print(f"UUID generado: {uuidkey}")
    print(f"Base URL MinIO: {baseurl}")

    # ----- Subir cada archivo base64 de `archivos` -----
    for arc in archivos:
        fn   = arc['file_name']
        data = base64.b64decode(arc['content'])
        key  = f"{id_mission}/{uuidkey}/{fn}"
        url  = f"{baseurl}/{bucket}/{key}"

        # determinar ContentType
        if fn.lower().endswith(('.tif', '.tiff')):
            ctype = 'image/tiff'
        elif fn.lower().endswith('.png'):
            ctype = 'image/png'
        elif fn.lower().endswith(('.jpg', '.jpeg')):
            ctype = 'image/jpeg'
        elif fn.lower().endswith('.csv'):
            ctype = 'text/csv'
        elif fn.lower().endswith('.pdf'):
            ctype = 'application/pdf'
        else:
            ctype = 'application/octet-stream'

        print(f"Subiendo archivo: {fn}")
        print(f" → Key en MinIO: {key}")
        print(f" → Content-Type: {ctype}")
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=io.BytesIO(data),
            ContentType=ctype
        )

        nuevos_paths[fn] = url
        print(f"Subido {fn} → {url}")

    # ----- Actualizar rutas en executionResources -----
    for resource in updated.get('executionResources', []):
        orig = os.path.basename(resource.get('path',''))
        print(f"\nProcesando resource original: {orig}")

        # Actualizar path principal
        if orig in nuevos_paths:
            old = resource['path']
            new = nuevos_paths[orig]
            resource['path'] = new
            print(f"resource['path'] actualizado:")
            print(f" - Antes: {old}")
            print(f" - Después: {new}")
        else:
            print(f"No hay nuevo path para {orig} en nuevos_paths.")

        # Actualizar thumbnails si los hay
        for entry in resource.get('data', []):
            val       = entry.get('value', {})
            thumb_b64 = val.get('thumbnail', {}).get('thumbRef')
            if not thumb_b64:
                continue

            thumb_name = os.path.splitext(orig)[0] + "_thumbnail.png"
            key_thumb  = f"{id_mission}/{uuidkey}/{thumb_name}"
            thumb_url  = f"{baseurl}/{bucket}/{key_thumb}"
            img        = base64.b64decode(thumb_b64)

            print(f" Subiendo thumbnail para {orig}: {thumb_name}")
            print(f"   - Key thumbnail: {key_thumb}")
            s3.put_object(
                Bucket=bucket,
                Key=key_thumb,
                Body=io.BytesIO(img),
                ContentType='image/png'
            )

            val['thumbnail']['path'] = thumb_url
            print(f"thumbnail.path actualizado:")
            print(f" - A: {thumb_url}")

    # ----- Subir el JSON modificado -----
    json_key = f"{id_mission}/{uuidkey}/algorithm_result.json"
    json_url = f"{baseurl}/{bucket}/{json_key}"
    print(f"\nSubiendo JSON actualizado:")
    print(f"  → Key JSON: {json_key}")
    s3.put_object(
        Bucket=bucket,
        Key=json_key,
        Body=io.BytesIO(json.dumps(updated).encode('utf-8')),
        ContentType='application/json'
    )
    print(f"JSON final subido → {json_url}")

    # ----- Historizar con el JSON actualizado -----
    print("Llamando a historizacion() con el JSON modificado.")
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