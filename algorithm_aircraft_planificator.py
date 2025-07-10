#@TODO IMPORTANTE: planning_id ahora ya no es sólo un número, sino varios (de varias misiones), por lo que hay que cambiar para que todo vaya bien con varios
# pero por el momento se puede probar en front con una sola misión.

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
import pandas as pd
import requests
import paramiko
import pytz
from sqlalchemy import text
from dag_utils import get_minio_client, get_db_session, minio_api
from requests.auth import HTTPBasicAuth
from utils.insert_end_of_execution import end_of_flow_task
from utils.callback_utils import task_failure_callback

# Constantes
EINFOREX_ROUTE = "/atcServices/rest/ResourcePlanningAlgorithmExecutionService/save"
INPUT_FILENAME = "input_data_aeronaves.txt"
SERVER_INPUT_DIR = "/algoritms/algoritmo-asignacion-aeronaves-objetivo-5/input/"
SERVER_OUTPUT_DIR = "/algoritms/algoritmo-asignacion-aeronaves-objetivo-5/output/"
MINIO_BUCKET = "tmp"
MINIO_FOLDER = "algorithm_aircraft_planificator_outputs" 
SERVER_EXECUTIONS_DIR = "/algoritms/executions/"


# Definición de la función para construir el payload para EINFOREX
def build_einforex_payload(mpd):
    # Mapeo a resourcePlanningCriteria
    resource_planning_criteria = [
        {
            "since":       int(c["dateFrom"] / 1000) * 1000,  # Eliminar milisegundos truncando el número /1000
            "until":       int(c["dateTo"] / 1000) * 1000,
            "aircrafts":   c["aircrafts"],
            "waterAmount": c["capacity"],
            "aircraftNum": c["numAircrafts"]
        }
        for c in mpd["criteria"]
    ]
    
    return {
        "startDate": int(mpd["startDate"] / 1000) * 1000,
        "endDate":   int(mpd["endDate"] / 1000) * 1000,
        "sinceDate": int(mpd["startDate"] / 1000) * 1000,
        "untilDate": int(mpd["endDate"] / 1000) * 1000,
        "fireLocation": {
            "srid": 4326,
            "x": mpd['lonLat'][0],
            "y": mpd['lonLat'][1],
            "z": 0
        },
        "availableAircrafts": mpd['aircrafts'],
        "outputInterval": mpd['timeInterval'],
        "resourcePlanningCriteria": resource_planning_criteria,
        "resourcePlanningResult": []
    }

# Definición de la función para obtener el planning_id de EINFOREX
def get_planning_id_from_einforex(payload):
    """
    Llama a la API de EINFOREX usando autenticación HTTP básica (usuario/contraseña) y devuelve el planning_id.
    """
    try:
        # Obtener conexión de Airflow
        connection = BaseHook.get_connection('einforex_planning_url')
        
        planning_url = connection.host + "/rest/ResourcePlanningAlgorithmExecutionService/save"
        username = connection.login
        password = connection.password

        print(f"[INFO] Llamando a {planning_url} con usuario {username}")
        print(json.dumps(payload, indent=2))

        response = requests.post(
            planning_url,
            json=payload,
            auth=HTTPBasicAuth(username, password),
            timeout=30
        )

        response.raise_for_status()
        response_data = response.json()
        print(f"[INFO] Respuesta de EINFOREX: {response_data}")
        
        planning_id = response.json().get('id')
        if planning_id is None:
            raise ValueError("La respuesta de EINFOREX no contiene 'id'")

        print(f"[INFO] Planning ID recibido: {planning_id}")
        return planning_id

    except Exception as e:
        print(f"[ERROR] Fallo al obtener planning_id de EINFOREX: {e}")
        raise


# Preparar y subir el input para ejecutar el algoritmo

def prepare_and_upload_input(**context):
    raw_conf = context['dag_run'].conf
    message = raw_conf.get('message')
    trace_id = context['dag_run'].conf['trace_id']
    print(f"Processing with trace_id: {trace_id}")
    user = message.get('from_user')
    context['ti'].xcom_push(key='user', value=user)

    input_data = message.get('input_data')
    if isinstance(input_data, str):
        input_data = json.loads(input_data)

    mission_data = input_data.get('missionData', [])
    assignment_id = input_data.get('assignmentId')
    context['ti'].xcom_push(key='assignment_id', value=assignment_id)

    planning_ids = []

    ssh_conn = BaseHook.get_connection("ssh_avincis_2")
    hostname, username = ssh_conn.host, ssh_conn.login
    ssh_key_decoded = Variable.get("ssh_avincis_p-2")
    execution_folder = f"EJECUCION_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    execution_path = SERVER_EXECUTIONS_DIR + execution_folder
    context['ti'].xcom_push(key='execution_path', value=execution_path)
    context['ti'].xcom_push(key='execution_folder', value=execution_folder)

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
        try:
            sftp.mkdir(execution_path)
            sftp.mkdir(f"{execution_path}/input")
            sftp.mkdir(f"{execution_path}/output")
            print(f"[INFO] Carpetas creadas en {execution_path}")
        except IOError:
            print(f"[WARN] Carpetas ya existentes: {execution_path}")

        for mission in mission_data:
            payload = build_einforex_payload(mission)
            planning_id = get_planning_id_from_einforex(payload)
            planning_ids.append(planning_id)

            input_content = f"""medios=a
url1=https://pre.atcservices.cirpas.gal/rest/ResourcePlanningAlgorithmExecutionService/get?id={planning_id}
url2=https://pre.atcservices.cirpas.gal/rest/FlightQueryService/searchByCriteria
url3=https://pre.atcservices.cirpas.gal/rest/FlightReportService/getReport
url4=https://pre.atcservices.cirpas.gal/rest/AircraftStatusService/getAll
url5=https://pre.atcservices.cirpas.gal/rest/AircraftBaseService/getAll
url6=https://pre.atcservices.cirpas.gal/rest/ResourcePlanningAlgorithmExecutionService/update
user=ITMATI.DES
password=Cui_1234
modelos_aeronave=Input/modelos_vehiculo.csv
"""
            remote_path = f"{execution_path}/input/{planning_id}_{INPUT_FILENAME}"
            with sftp.file(remote_path, 'w') as remote_file:
                remote_file.write(input_content)
                print(f"[INFO] Archivo subido para planning_id {planning_id}")

    finally:
        sftp.close()
        client.close()
        bastion.close()
        os.remove(temp_key_path)
        print("[INFO] Conexiones SSH cerradas y clave eliminada.")

    context['ti'].xcom_push(key='planning_ids', value=planning_ids)
    print(f"[INFO] Todos los planning_ids enviados: {planning_ids}")

# Definición de la función para ejecutar el algoritmo y descargar el resultado
def run_and_download_algorithm(**context):
    print("[INFO] Iniciando ejecución de run_and_download_algorithm...")

    trace_id = context['dag_run'].conf['trace_id']
    print(f"Processing with trace_id: {trace_id}")

    ssh_conn = BaseHook.get_connection("ssh_avincis_2")
    hostname, username = ssh_conn.host, ssh_conn.login
    print(f"[INFO] Conectando a bastión {hostname} como {username}")
    ssh_key_decoded = Variable.get("ssh_avincis_p-2")

    # obtenemos LOS planningIds de contexto
    planning_ids = context['ti'].xcom_pull(key='planning_ids')

    execution_path = context['ti'].xcom_pull(key='execution_path')
    if not execution_path:
        raise Exception("[ERROR] No se encontró execution_path en XComs")


    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file_key:
        temp_file_key.write(ssh_key_decoded)
        temp_key_path = temp_file_key.name
    os.chmod(temp_key_path, 0o600)

    try:
        # SSH al bastión
        bastion = paramiko.SSHClient()
        bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        bastion.connect(hostname=hostname, username=username, key_filename=temp_key_path)
        print("[INFO] Conexión SSH establecida con el bastión")

        # SSH interno
        jump = bastion.get_transport().open_channel(
            "direct-tcpip", dest_addr=("10.38.9.6", 22), src_addr=("127.0.0.1", 0)
        )
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname="10.38.9.6", username="airflow-executor", sock=jump, key_filename=temp_key_path)
        print("[INFO] Conexión SSH establecida con el servidor interno (10.38.9.6)")

        # Ejecutar algoritmo PARA CADA PLANNING_ID
        for planning_id in planning_ids:
            input_path = f"{execution_path}/input/{planning_id}_{INPUT_FILENAME}"
            print(f"[INFO] Usando input_path: {input_path}")
            cmd = f'cd /algoritms/algoritmo-recomendador-objetivo-5 && python3 call_recomendador.py {input_path}'
            print(f"[INFO] Ejecutando comando remoto: {cmd}")
            stdin, stdout, stderr = client.exec_command(cmd)

            print("[INFO] --- Output STDOUT del algoritmo ---")
            for line in iter(stdout.readline, ""):
                if line:
                    print(f"[REMOTE STDOUT] {line.strip()}")

            print("[INFO] --- Output STDERR del algoritmo ---")
            for line in iter(stderr.readline, ""):
                if line:
                    print(f"[REMOTE STDERR] {line.strip()}")

            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise Exception(f"[ERROR] Algoritmo terminó con error. Exit status: {exit_status}")

            print("[INFO] Algoritmo ejecutado correctamente. No se descarga ningún JSON; se usará EINFOREX.")

    except Exception as e:
        print(f"[ERROR] Error durante la ejecución de run_and_download_algorithm: {e}")
        raise

    finally:        
        client.close()
        bastion.close()
        print("[INFO] Conexiones SSH cerradas correctamente.")
        if os.path.exists(temp_key_path):
            os.remove(temp_key_path)
            print(f"[INFO] Clave SSH temporal eliminada: {temp_key_path}")



# AQUÍ TAMBIÉN GUARDAR YA DIRECTAMENTE EL JSON EN FICHERO Y AU
# Para cada planning ID:
#   TRAER results de BBDD, guardar el json, subir a minio el json, guardar en histórico, pasar a csv el json, notificar de vuelta
#
def fetch_results_from_einforex(**context):
    from requests.auth import HTTPBasicAuth

    print("[INFO] Iniciando fetch_results_from_einforex...")


    planning_ids = context['ti'].xcom_pull(task_ids='prepare_and_upload_input', key='planning_ids')
    if not planning_ids or not isinstance(planning_ids, list):
        raise ValueError("[ERROR] No se encontraron planning_ids válidos en XCom")
    
    connection = BaseHook.get_connection('einforex_planning_url')
    username = connection.login
    password = connection.password
    
    s3_client = get_minio_client()
    base_url = minio_api()
    user = context['ti'].xcom_pull(key='user')

    csv_urls = []
    for planning_id in planning_ids:
        url = f"{connection.host}/rest/ResourcePlanningAlgorithmExecutionService/get?id={planning_id}"
        print(f"[INFO] Llamando a EINFOREX para resultados con ID: {planning_id}")
        print(f"[INFO] URL: {url}")

        try:
            response = requests.get(url, auth=HTTPBasicAuth(username, password), timeout=30)
            response.raise_for_status()
            json_content = response.json()

            print("[INFO] Resultados del algoritmo obtenidos correctamente.")
            print("[INFO] Ejemplo de resultado:")
            print(json.dumps(json_content, indent=2))

            #context['ti'].xcom_push(key='einforex_result', value=result_data)


            json_filename = f"einforex_result_{planning_id}.json"

            with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.json') as tmp_file:
                tmp_file.write(json.dumps(json_content, ensure_ascii=False))
                tmp_file_path = tmp_file.name

            key_current = f"{MINIO_FOLDER}/jsons/{json_filename}"
            timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
            key_historic = f"{MINIO_FOLDER}/historic/{timestamp}_{json_filename}"
            s3_client.upload_file(tmp_file_path, MINIO_BUCKET, key_current)
            s3_client.upload_file(tmp_file_path, MINIO_BUCKET, key_historic)

            json_url = f"{base_url}/{MINIO_BUCKET}/{key_current}"
           # ya no hace falta context['ti'].xcom_push(key='json_url', value=json_url)

            intervals = json_content.get("resourcePlanningResult", [])
            aircrafts = list(set(json_content.get("availableAircrafts", [])))

            if not intervals or not aircrafts:
                raise ValueError("[ERROR] El JSON no contiene intervals o aircrafts válidos")

            # Formateo estético: convertir intervalos de milisegundos a HH:MM:SS
            madrid_tz = pytz.timezone('Europe/Madrid')
            headers = [
                f"{datetime.fromtimestamp(i['since'] / 1000, madrid_tz).strftime('%H:%M:%S')} - "
                f"{datetime.fromtimestamp(i['until'] / 1000, madrid_tz).strftime('%H:%M:%S')}"
                for i in intervals
            ]
            table_data = {aircraft: [] for aircraft in aircrafts}
            for i in intervals:
                active = i.get("aircrafts", [])
                for aircraft in aircrafts:
                    table_data[aircraft].append("YES" if aircraft in active else "NO")

            df = pd.DataFrame(table_data, index=headers).transpose()

            # Guardar como CSV
            csv_filename = json_filename.replace('.json', '_table.csv')
            local_csv_path = f"/tmp/{csv_filename}"
            df.to_csv(local_csv_path)

            csv_key = f"{MINIO_FOLDER}/outputs/{csv_filename}"
            print(f"[INFO] Subiendo CSV a MinIO: {csv_key}")
            s3_client.upload_file(local_csv_path, MINIO_BUCKET, csv_key)
            csv_urls.append(f"{base_url}/{MINIO_BUCKET}/{csv_key}")

            # Payload opcional
            # if local_payload_json and os.path.exists(local_payload_json):
            #     payload_key = f"{MINIO_FOLDER}/inputs/{os.path.basename(local_payload_json)}"
            #     s3_client.upload_file(local_payload_json, bucket, payload_key)
            #     payload_url = f"{base_url}/{bucket}/{payload_key}"
            #     context['ti'].xcom_push(key='payload_json_url', value=payload_url)

            # Historial en BBDD
            session = get_db_session()
            madrid_tz = pytz.timezone('Europe/Madrid')
            assignment_id = json_content.get("assignmentId")
            sampled_feature = int(assignment_id) if assignment_id is not None else None

            session.execute(text("""
                INSERT INTO algoritmos.algoritmo_aircraft_planificator (
                    sampled_feature, result_time, phenomenon_time, input_data, output_data
                ) VALUES (
                    :sampled_feature, :result_time, :phenomenon_time, :input_data, :output_data
                )
            """), {
                "sampled_feature": sampled_feature,
                "result_time": datetime.now(madrid_tz),
                "phenomenon_time": datetime.now(madrid_tz),
                "input_data": json.dumps({}, ensure_ascii=False),
                "output_data": json.dumps(json_content, ensure_ascii=False)
            })
            session.commit()
        except Exception as e:
            print(f"[ERROR] Fallo al obtener resultados desde EINFOREX: {e}")
            raise

    ## fuera del bucle FOR

    # Notificación al frontend
    planning_ids_str = ', '.join(map(str, planning_ids))  # Convertir planning_ids a una cadena separada por comas
    payload = {
        "to": user,
        "actions": [
            {
                "type": "loadChart",
                "data": {
                    "url": csv_urls,
                    "button": {
                        "key": "openPlanner",
                        "data": json_url
                },
                    "title": f"Planificación de aeronaves con ids: {planning_ids_str}",
                }
            },
            {
                "type": "notify",
                "data": {
                    "button": "Planificación de aeronaves",
                    "message": f"Resultados del algoritmo de planificación disponibles. IDs planificación: {planning_ids_str}"
                }
            }
        ]
    }

    try:
        result = session.execute(text("""
            INSERT INTO public.notifications (destination, "data", "date", status)
            VALUES ('ignis', :data, :date, NULL)
            RETURNING id
        """), {'date': datetime.utcnow(), 'data': json.dumps(payload, ensure_ascii=False)})

        session.commit()
        print("[INFO] Notificación al frontend registrada.")

    except Exception as e:
        print(f"[ERROR] Fallo al notificar: {e}")
        raise

# Definición de la función para notificar al frontend y guardar en la base de datos

def always_save_logs(**context):
    print("[INFO] always_save_logs ejecutado.")
    return True

# Definición DAG

default_args = {
    'owner': 'oscar',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': task_failure_callback
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

fetch_result_task = PythonOperator(
    task_id='fetch_results_from_einforex',
    python_callable=fetch_results_from_einforex,
    provide_context=True,
    dag=dag
)


# Definir la secuencia de tareas y dependencias

prepare_task >> run_task >> fetch_result_task

