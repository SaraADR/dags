# TENEMOS LA FUNCION DE PRUEBA PARA NOTIFICAR AL FRONTEND, UNA VEZ ESTE COMPLETADO EL ENVÍO Y SE REALICE CORRECTAMENTE
# SIMPLEMENTE INTEGRARÍAMOS DENTRO DE LA FUNCION ESTE PROCESO, Y ACABAR CON RESUMEN DEL FLUJO:

# GENERACION DE DATOS DESDE EL FRONT > EJECUCIÓN DEL ALGORITMO EN SERVIDOR > DESCARGA DE RESULTADOS > ENVÍO DE NOTIFICACIÓN AL FRONTEND 
# > ENVÍO DE NOTIFICACIÓN A LA BASE DE DATOS

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import tempfile
import base64
import json
import csv
import requests
import paramiko
import pytz
from sqlalchemy import text
from dag_utils import get_minio_client, get_db_session

# Constantes
EINFOREX_ROUTE = "/atcServices/rest/ResourcePlanningAlgorithmExecutionService/save"
INPUT_FILENAME = "input_data_aeronaves.txt"
SERVER_INPUT_DIR = "/algoritms/algoritmo-asignacion-aeronaves-objetivo-5/input/"
SERVER_OUTPUT_DIR = "/algoritms/algoritmo-asignacion-aeronaves-objetivo-5/output/"
MINIO_BUCKET = "tmp"
MINIO_FOLDER = "algorithm_aircraft_planificator_outputs"

# Funciones auxiliares

def to_millis(dt):
    return int(dt.timestamp() * 1000)

def build_einforex_payload(fire, vehicles, assignment_criteria):
    now = datetime.utcnow()
    start_dt = now.replace(minute=((now.minute // 10 + 1) * 10) % 60, second=0, microsecond=0)
    end_dt = start_dt + timedelta(hours=4)

    fire_id = fire['id']
    matched_criteria = [c for c in assignment_criteria if c['fireId'] == fire_id]

    available_aircrafts = []
    if matched_criteria:
        for model in matched_criteria[0]['vehicleModels']:
            matched_vehicles = [v for v in vehicles if v['model'] == model]
            available_aircrafts.extend([v['id'] for v in matched_vehicles])

    return {
        "startDate": None,
        "endDate": None,
        "sinceDate": to_millis(start_dt),
        "untilDate": to_millis(end_dt),
        "fireLocation": {
            "srid": fire['position']['srid'],
            "x": fire['position']['x'],
            "y": fire['position']['y'],
            "z": fire['position'].get('z', 0)
        },
        "availableAircrafts": available_aircrafts,
        "outputInterval": None,
        "resourcePlanningCriteria": [{
            "id": 0,
            "executionId": 0,
            "since": to_millis(start_dt),
            "until": to_millis(end_dt),
            "aircrafts": available_aircrafts,
            "waterAmount": None,
            "aircraftNum": None
        }],
        "resourcePlanningResult": []
    }

def get_planning_id_from_einforex(payload):
    """
    Llama a la API de EINFOREX para guardar la planificación y devuelve el planning_id.
    """
    try:
        connection = BaseHook.get_connection('einforex_planning_url')
        extra = json.loads(connection.extra)
        planning_url = extra['planning_url']

        response = requests.post(planning_url, json=payload, timeout=30)
        response.raise_for_status()
        
        planning_id = response.json().get('id')
        if planning_id is None:
            raise ValueError("La respuesta no contiene 'id'")

        print(f"[INFO] Planning ID recibido: {planning_id}")
        return planning_id

    except Exception as e:
        print(f"[ERROR] Fallo al obtener planning_id de EINFOREX: {e}")
        raise


# Tareas principales

def prepare_and_upload_input(**context):
    # Cargar correctamente el conf
    raw_conf = context['dag_run'].conf
    if isinstance(raw_conf, str):
        raw_conf = json.loads(raw_conf)

    message = raw_conf.get('message')
    if not message:
        raise ValueError("El input no contiene el campo 'message'.")

    user = message.get('from_user')

    raw_input_data = message.get('input_data')
    if not raw_input_data:
        raise ValueError("El input no contiene 'input_data' dentro de message.")

    if isinstance(raw_input_data, str):
        input_data = json.loads(raw_input_data)
    else:
        input_data = raw_input_data

    # Confirmar input recibido
    print("[DEBUG] INPUT DATA FINAL:", input_data)

    vehicles = input_data['vehicles']
    fires = input_data['fires']
    assignment_criteria = input_data['assignmentCriteria']

    # Guardamos usuario para usar en notify_frontend
    context['ti'].xcom_push(key='user', value=user)

    # Conexión SSH
    ssh_conn = BaseHook.get_connection("ssh_avincis_2")
    hostname, username = ssh_conn.host, ssh_conn.login
    ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p-2")).decode("utf-8")

    input_lines = []
    for fire in fires:
        payload = build_einforex_payload(fire, vehicles, assignment_criteria)
        planning_id = get_planning_id_from_einforex(payload)
        line = f"http://10.38.9.6:8080{EINFOREX_ROUTE}\n{planning_id}\n/home/airflow/modelos_vehiculo.csv\n"
        input_lines.append(line)

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file_key:
        temp_file_key.write(ssh_key_decoded)
        temp_key_path = temp_file_key.name
    os.chmod(temp_key_path, 0o600)

    try:
        bastion = paramiko.SSHClient()
        bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        bastion.connect(hostname=hostname, username=username, key_filename=temp_key_path)

        jump = bastion.get_transport().open_channel("direct-tcpip", dest_addr=("10.38.9.6", 22), src_addr=("127.0.0.1", 0))
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname="10.38.9.6", username="airflow-executor", sock=jump, key_filename=temp_key_path)

        sftp = client.open_sftp()
        with sftp.file(SERVER_INPUT_DIR + INPUT_FILENAME, 'w') as remote_file:
            for line in input_lines:
                remote_file.write(line)

        sftp.close()
        client.close()
        bastion.close()
        print("[INFO] Input preparado y subido correctamente")

    finally:
        os.remove(temp_key_path)


def run_and_download_algorithm(**context):
    ssh_conn = BaseHook.get_connection("ssh_avincis_2")
    hostname, username = ssh_conn.host, ssh_conn.login
    ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p-2")).decode("utf-8")

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file_key:
        temp_file_key.write(ssh_key_decoded)
        temp_key_path = temp_file_key.name
    os.chmod(temp_key_path, 0o600)

    try:
        bastion = paramiko.SSHClient()
        bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        bastion.connect(hostname=hostname, username=username, key_filename=temp_key_path)

        jump = bastion.get_transport().open_channel("direct-tcpip", dest_addr=("10.38.9.6", 22), src_addr=("127.0.0.1", 0))
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname="10.38.9.6", username="airflow-executor", sock=jump, key_filename=temp_key_path)

        cmd = 'cd /algoritms/algoritmo-asignacion-aeronaves-objetivo-5 && python3 call_recomendador.py input/input_data_aeronaves.txt'
        client.exec_command(cmd)
        print("[INFO] Algoritmo ejecutado remotamente")

        sftp = client.open_sftp()
        output_files = sftp.listdir(SERVER_OUTPUT_DIR)
        json_filename = next((f for f in output_files if f.endswith('.json')), None)
        if not json_filename:
            raise Exception("[ERROR] No se encontró JSON de salida")

        local_tmp_output = tempfile.NamedTemporaryFile(delete=False).name
        sftp.get(SERVER_OUTPUT_DIR + json_filename, local_tmp_output)

        context['ti'].xcom_push(key='json_filename', value=json_filename)
        context['ti'].xcom_push(key='local_tmp_output', value=local_tmp_output)

        sftp.close()
        client.close()
        bastion.close()
        print("[INFO] Output descargado correctamente")
    finally:
        os.remove(temp_key_path)

def process_outputs(**context):
    json_filename = context['ti'].xcom_pull(key='json_filename')
    local_tmp_output = context['ti'].xcom_pull(key='local_tmp_output')

    s3_client = get_minio_client()
    bucket = MINIO_BUCKET
    folder = MINIO_FOLDER
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    key_current = f"{folder}/jsons/{json_filename}"
    key_historic = f"{folder}/historic/{timestamp}_{json_filename}"

    s3_client.upload_file(local_tmp_output, bucket, key_current)
    s3_client.upload_file(local_tmp_output, bucket, key_historic)

    json_url = f"https://minio.avincis.cuatrodigital.com/{bucket}/{key_current}"
    context['ti'].xcom_push(key='json_url', value=json_url)

    with open(local_tmp_output, 'r', encoding='utf-8') as f:
        output_data = json.load(f)

    local_csv_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
    with open(local_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['assignment_id', 'vehicle_id'])
        for assignment in output_data.get('assignments', []):
            for vehicle in assignment.get('vehicles', []):
                writer.writerow([assignment['id'], vehicle])

    s3_client.upload_file(local_csv_file, bucket, f"{folder}/{os.path.basename(local_csv_file)}")
    csv_url = f"https://minio.avincis.cuatrodigital.com/{bucket}/{folder}/{os.path.basename(local_csv_file)}"
    context['ti'].xcom_push(key='csv_url', value=csv_url)

    session = get_db_session()
    madrid_tz = pytz.timezone('Europe/Madrid')
    try:
        session.execute(text("""
            INSERT INTO algoritmos.algoritmo_aircraft_planificator (sampled_feature, result_time, phenomenon_time, input_data, output_data)
            VALUES (:sampled_feature, :result_time, :phenomenon_time, :input_data, :output_data)
        """), {
            "sampled_feature": output_data.get("assignmentId", "unknown"),
            "result_time": datetime.now(madrid_tz),
            "phenomenon_time": datetime.now(madrid_tz),
            "input_data": json.dumps({}),
            "output_data": json.dumps(output_data)
        })
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    os.remove(local_tmp_output)
    os.remove(local_csv_file)
    print("[INFO] Output subido a MinIO y registrado en histórico")

def notify_frontend(**context):
    user = context['ti'].xcom_pull(key='user')
    csv_url = context['ti'].xcom_pull(key='csv_url')
    json_url = context['ti'].xcom_pull(key='json_url')

    session = get_db_session()
    now_utc = datetime.now(pytz.utc)
    payload = {
        "to": user,
        "actions": [
            {"type": "load_csv_table", "data": {"message": "Resultados disponibles."}},
            {"type": "loadTable", "data": {"url": csv_url}},
            {"type": "loadJson", "data": {"url": json_url, "message": "Descargar JSON de resultados."}}
        ]
    }
    try:
        session.execute(text("""
            INSERT INTO public.notifications (destination, "data", "date", status)
            VALUES ('ignis', :data, :date, NULL)
        """), {'data': json.dumps(payload), 'date': now_utc})
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
    print("[INFO] Notificación enviada al frontend")

# Definición DAG
default_args = {
    'owner': 'oscar',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'algorithm_aircraft_planificator',
    default_args=default_args,
    description='DAG Planificador aeronaves',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

prepare_task = PythonOperator(
    task_id='prepare_and_upload_input',
    python_callable=prepare_and_upload_input,
    provide_context=True,
    dag=dag
)

run_task = PythonOperator(
    task_id='run_and_download_algorithm',
    python_callable=run_and_download_algorithm,
    provide_context=True,
    dag=dag
)

process_task = PythonOperator(
    task_id='process_outputs',
    python_callable=process_outputs,
    provide_context=True,
    dag=dag
)

notify_task = PythonOperator(
    task_id='notify_frontend',
    python_callable=notify_frontend,
    provide_context=True,
    dag=dag
)

prepare_task >> run_task >> process_task >> notify_task





































# # DAG para ejecutar el algoritmo de planificación de aeronaves, descargar el resultado y notificar al frontend

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.models import Variable
# from airflow.hooks.base import BaseHook
# from datetime import datetime, timedelta
# import os
# import tempfile
# import base64
# import json
# import csv
# import paramiko
# import pytz
# from sqlalchemy import text
# from dag_utils import get_minio_client, get_db_session

# def insert_notification(payload):
#     session = get_db_session()
#     try:
#         now_utc = datetime.now(pytz.utc)
#         query = text("""
#             INSERT INTO public.notifications
#             (destination, "data", "date", status)
#             VALUES (:destination, :data, :date, NULL);
#         """)
#         session.execute(query, {
#             'destination': 'ignis',
#             'data': json.dumps(payload, ensure_ascii=False),
#             'date': now_utc
#         })
#         session.commit()
#     except Exception as e:
#         session.rollback()
#         print(f"Error insertando notificación: {str(e)}")
#         raise
#     finally:
#         session.close()

# def insert_history(output_data):
#     session = get_db_session()
#     try:
#         madrid_tz = pytz.timezone('Europe/Madrid')
#         datos = {
#             "sampled_feature": output_data.get("assignmentId", "unknown"),
#             "result_time": datetime.now(madrid_tz),
#             "phenomenon_time": datetime.now(madrid_tz),
#             "input_data": json.dumps({}),  # De momento vacío (se puede completar si tienes input)
#             "output_data": json.dumps(output_data)
#         }
#         query = text("""
#             INSERT INTO algoritmos.algoritmo_aircraft_planificator (
#                 sampled_feature, result_time, phenomenon_time, input_data, output_data
#             ) VALUES (
#                 :sampled_feature, :result_time, :phenomenon_time, :input_data, :output_data
#             )
#         """)
#         session.execute(query, datos)
#         session.commit()
#     except Exception as e:
#         session.rollback()
#         print(f"Error insertando histórico: {str(e)}")
#         raise
#     finally:
#         session.close()

# def process_and_notify_with_csv():
#     output_file = "test1.1.json"
#     user = "all_users"

#     ssh_conn = BaseHook.get_connection("ssh_avincis_2")
#     hostname = ssh_conn.host
#     username = ssh_conn.login

#     ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p-2")).decode("utf-8")
#     with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
#         temp_file.write(ssh_key_decoded)
#         temp_file_path = temp_file.name
#     os.chmod(temp_file_path, 0o600)

#     try:
#         bastion = paramiko.SSHClient()
#         bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#         bastion.connect(hostname=hostname, username=username, key_filename=temp_file_path)

#         jump_transport = bastion.get_transport()
#         jump_channel = jump_transport.open_channel(
#             "direct-tcpip",
#             dest_addr=("10.38.9.6", 22),
#             src_addr=("127.0.0.1", 0)
#         )

#         target_client = paramiko.SSHClient()
#         target_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#         target_client.connect(
#             hostname="10.38.9.6",
#             username="airflow-executor",
#             sock=jump_channel,
#             key_filename=temp_file_path
#         )

#         sftp = target_client.open_sftp()

#         remote_output_path = '/algoritms/algoritmo-asignacion-aeronaves-objetivo-5/output/test1.1.json'
#         local_tmp_output = tempfile.NamedTemporaryFile(delete=False).name

#         sftp.get(remote_output_path, local_tmp_output)

#         sftp.close()
#         target_client.close()
#         bastion.close()

#     finally:
#         os.remove(temp_file_path)

#     # Leer JSON descargado
#     with open(local_tmp_output, 'r', encoding='utf-8') as f:
#         output_data = json.load(f)

#     # Subir JSON a MinIO
#     s3_client = get_minio_client()
#     bucket_name = "tmp"
#     folder = "algorithm_outputs"
    
#     json_key_current = f"{folder}/jsons/{output_file}"
#     timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
#     json_key_historic = f"{folder}/historic/{timestamp}_{output_file}"

#     # Subida actual y subida histórica
#     s3_client.upload_file(local_tmp_output, bucket_name, json_key_current)
#     s3_client.upload_file(local_tmp_output, bucket_name, json_key_historic)

#     minio_base_url = "https://minio.avincis.cuatrodigital.com/"
#     json_url = f"{minio_base_url}/{bucket_name}/{json_key_current}"

#     os.remove(local_tmp_output)

#     # Crear CSV temporal
#     assignment_id = output_data.get('assignmentId')
#     assignments_list = output_data.get('assignments', [])

#     local_csv_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name

#     with open(local_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
#         writer = csv.writer(csvfile)
#         writer.writerow(['assignment_id', 'vehicle_id'])
#         for assignment in assignments_list:
#             aid = assignment.get('id')
#             vehicles = assignment.get('vehicles', [])
#             for vehicle in vehicles:
#                 writer.writerow([aid, vehicle])

#     # Subir CSV a MinIO
#     csv_key = f"{folder}/{os.path.basename(local_csv_file)}"
#     s3_client.upload_file(local_csv_file, bucket_name, csv_key)

#     csv_url = f"{minio_base_url}/{bucket_name}/{csv_key}"

#     os.remove(local_csv_file)

#     # Guardar histórico en la base de datos
#     insert_history(output_data)

#     # Insertar notificación para el Front
#     payload = {
#         "to": user,
#         "actions": [
#             {
#                 "type": "load_csv_table",
#                 "data": {
#                     "message": "Datos generados por el algoritmo de planificación de aeronaves disponibles."
#                 }
#             },
#             {
#                 "type": "loadTable",
#                 "data": {
#                     "url": csv_url
#                 }
#             },
#             {
#                 "type": "loadJson",
#                 "data": {
#                     "url": json_url,
#                 }
#             }
#         ]
#     }
#     insert_notification(payload)

# default_args = {
#     'owner': 'oscar',
#     'depends_on_past': False,
#     'start_date': datetime(2025, 1, 3),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=1),
# }

# dag = DAG(
#     'algorithm_aircraft_planificator',
#     default_args=default_args,
#     description='DAG completo: descarga test1.1.json, sube CSV y JSON, guarda histórico en BD y notifica al frontend',
#     schedule_interval=None,
#     catchup=False,
#     max_active_runs=1,
#     concurrency=1
# )

# process_fixed_output_task = PythonOperator(
#     task_id='execute_algorithm_planificator',
#     python_callable=process_and_notify_with_csv,
#     dag=dag,
# )


































# VERSION ANTIGUA 

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import json
# import base64
# import tempfile
# import os
# import paramiko
# from airflow.models import Variable
# from airflow.hooks.base import BaseHook

# def execute_algorithm_remote(**context):
#     message = context['dag_run'].conf
#     input_data_str = message['message']['input_data']
#     if isinstance(input_data_str, str):
#         input_data = json.loads(input_data_str)
#     else:
#         input_data = input_data_str
#     print(input_data)

#     ssh_conn = BaseHook.get_connection("ssh_avincis_2")
#     hostname = ssh_conn.host
#     username = ssh_conn.login

#     ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p-2")).decode("utf-8")
#     with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
#         temp_file.write(ssh_key_decoded)
#         temp_file_path = temp_file.name
#     os.chmod(temp_file_path, 0o600)

#     try:
#         bastion = paramiko.SSHClient()
#         bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#         bastion.connect(hostname=hostname, username=username, key_filename=temp_file_path)

#         jump_transport = bastion.get_transport()
#         jump_channel = jump_transport.open_channel(
#             "direct-tcpip",
#             dest_addr=("10.38.9.6", 22),
#             src_addr=("127.0.0.1", 0)
#         )

#         target_client = paramiko.SSHClient()
#         target_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#         target_client.connect(
#             hostname="10.38.9.6",
#             username="airflow-executor",
#             sock=jump_channel,
#             key_filename=temp_file_path
#         )

#         sftp = target_client.open_sftp()

#         # Input fijo esperado por el algoritmo
#         remote_input_path = '/algoritms/algoritmo-recomendador-objetivo-5/Input/input_data_aeronaves.txt'

#         # GENERACIÓN DEL CONTENIDO .TXT A PARTIR DE input_data
#         missions = input_data.get('missionData', [])
#         lines = []
#         for m in missions:
#             line = f"{m['missionID']},{m['lonLat'][0]},{m['lonLat'][1]},{m['startDate']},{m['endDate']},{m['timeInterval']}"
#             lines.append(line)
#         txt_content = "\n".join(lines)

#         with sftp.file(remote_input_path, 'w') as remote_file:
#             remote_file.write(txt_content)

#         sftp.close()

#         cmd = (
#             'cd /algoritms/algoritmo-recomendador-objetivo-5 && '
#             'python3 call_recomendador.py Input/input_data_aeronaves.txt'
#         )

#         stdin, stdout, stderr = target_client.exec_command(cmd)
#         print(stdout.read().decode())
#         print(stderr.read().decode())

#         target_client.close()
#         bastion.close()

#     finally:
#         os.remove(temp_file_path)

# default_args = {
#     'owner': 'oscar',
#     'depends_on_past': False,
#     'start_date': datetime(2025, 1, 3),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=1),
# }

# dag = DAG(
#     'algorithm_aircraft_planificator',
#     default_args=default_args,
#     description='Ejecuta algoritmo de planificación en servidor Avincis',
#     schedule_interval=None,
#     catchup=False,
#     max_active_runs=1,
#     concurrency=1
# )

# process_element_task = PythonOperator(
#     task_id='execute_algorithm_planificator',
#     python_callable=execute_algorithm_remote,
#     dag=dag,
# )

# VERSION NUEVA CON SUBIDA AL FRONT

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.hooks.base import BaseHook
# from airflow.models import Variable
# from datetime import datetime, timedelta
# import paramiko
# import os
# import tempfile
# import base64
# import json
# import pandas as pd
# from sqlalchemy import create_engine, text
# from dag_utils import get_db_session
# import pytz

# def insert_notification(payload):
#     try:
#         session = get_db_session()
#         engine = session.get_bind()

#         time = datetime.now(pytz.utc)

#         query = text("""
#             INSERT INTO public.notifications
#             (destination, "data", "date", status)
#             VALUES (:destination, :data, :date, NULL);
#         """)
#         session.execute(query, {
#             'destination': 'ignis',
#             'data': json.dumps(payload, ensure_ascii=False),
#             'date': time
#         })
#         session.commit()

#     except Exception as e:
#         session.rollback()
#         print(f"Error insertando notificación: {str(e)}")
#     finally:
#         session.close()

# def notify_assignment_table(data, user):
#     headers = ["assignment_id", "vehicle_id"]
#     rows = []

#     for assignment in data.get('assignments', []):
#         assignment_id = assignment.get('id')
#         vehicles = assignment.get('vehicles', [])
#         for vehicle in vehicles:
#             rows.append([assignment_id, vehicle])

#     payload = {
#         "to": str(user),
#         "actions": [
#             {
#                 "type": "loadTable",
#                 "data": {
#                     "headers": headers,
#                     "rows": rows
#                 }
#             },
#             {
#                 "type": "notify",
#                 "data": {
#                     "message": "Resultados de asignación de vehículos disponibles."
#                 }
#             }
#         ]
#     }

#     insert_notification(payload)

# def execute_algorithm_remote(**context):
#     message = context['dag_run'].conf
#     input_data_str = message['message']['input_data']
#     user = message['message']['from_user']

#     if isinstance(input_data_str, str):
#         input_data = json.loads(input_data_str)
#     else:
#         input_data = input_data_str

#     ssh_conn = BaseHook.get_connection("ssh_avincis_2")
#     hostname = ssh_conn.host
#     username = ssh_conn.login

#     ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p-2")).decode("utf-8")
#     with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
#         temp_file.write(ssh_key_decoded)
#         temp_file_path = temp_file.name
#     os.chmod(temp_file_path, 0o600)

#     try:
#         bastion = paramiko.SSHClient()
#         bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#         bastion.connect(hostname=hostname, username=username, key_filename=temp_file_path)

#         jump_transport = bastion.get_transport()
#         jump_channel = jump_transport.open_channel(
#             "direct-tcpip",
#             dest_addr=("10.38.9.6", 22),
#             src_addr=("127.0.0.1", 0)
#         )

#         target_client = paramiko.SSHClient()
#         target_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#         target_client.connect(
#             hostname="10.38.9.6",
#             username="airflow-executor",
#             sock=jump_channel,
#             key_filename=temp_file_path
#         )

#         sftp = target_client.open_sftp()

#         remote_input_path = '/algoritms/algoritmo-recomendador-objetivo-5/Input/input_data_aeronaves.txt'

#         missions = input_data.get('missionData', [])
#         lines = []
#         for m in missions:
#             line = f"{m['missionID']},{m['lonLat'][0]},{m['lonLat'][1]},{m['startDate']},{m['endDate']},{m['timeInterval']}"
#             lines.append(line)
#         txt_content = "\n".join(lines)

#         with sftp.file(remote_input_path, 'w') as remote_file:
#             remote_file.write(txt_content)

#         cmd = (
#             'cd /algoritms/algoritmo-recomendador-objetivo-5 && '
#             'python3 call_recomendador.py Input/input_data_aeronaves.txt'
#         )

#         stdin, stdout, stderr = target_client.exec_command(cmd)
#         print(stdout.read().decode())
#         print(stderr.read().decode())

#         remote_output_path = '/algoritms/algoritmo-recomendador-objetivo-5/Output/output_file.csv'
#         local_tmp_output = tempfile.NamedTemporaryFile(delete=False).name

#         sftp.get(remote_output_path, local_tmp_output)

#         sftp.close()
#         target_client.close()
#         bastion.close()

#     finally:
#         os.remove(temp_file_path)

#     # Procesar Output
#     df = pd.read_csv(local_tmp_output)
#     output_data_json = df.to_dict(orient='records')
#     os.remove(local_tmp_output)

#     # Preparar estructura para enviar como tabla si es de tipo assignment
#     if 'assignmentId' in message['message']:
#         assignment_data = {
#             "assignmentId": message['message']['assignmentId'],
#             "assignments": output_data_json
#         }
#         notify_assignment_table(assignment_data, user)
#     else:
#         # Notificación general (no tabla)
#         payload = {
#             "to": str(user),
#             "actions": [
#                 {
#                     "type": "notify",
#                     "data": {
#                         "message": "Resultados de planificación de aeronaves disponibles."
#                     }
#                 }
#             ]
#         }
#         insert_notification(payload)

# default_args = {
#     'owner': 'oscar',
#     'depends_on_past': False,
#     'start_date': datetime(2025, 1, 3),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=1),
# }

# dag = DAG(
#     'algorithm_aircraft_planificator',
#     default_args=default_args,
#     description='Ejecuta algoritmo de planificación, genera tabla y notifica al Frontend',
#     schedule_interval=None,
#     catchup=False,
#     max_active_runs=1,
#     concurrency=1
# )

# process_element_task = PythonOperator(
#     task_id='execute_algorithm_planificator',
#     python_callable=execute_algorithm_remote,
#     provide_context=True,
#     dag=dag,
# )
