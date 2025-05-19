import pytz
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from dag_utils import get_minio_client
from airflow.models import Variable
import uuid
import io
import json


def process_json(**kwargs):
    json_content_original = kwargs['dag_run'].conf.get('json')

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