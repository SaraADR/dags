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
from requests.auth import HTTPBasicAuth

# Constantes
EINFOREX_ROUTE = "/atcServices/rest/ResourcePlanningAlgorithmExecutionService/save"
INPUT_FILENAME = "input_data_aeronaves.txt"
SERVER_INPUT_DIR = "/algoritms/algoritmo-asignacion-aeronaves-objetivo-5/input/"
SERVER_OUTPUT_DIR = "/algoritms/algoritmo-asignacion-aeronaves-objetivo-5/output/"
MINIO_BUCKET = "tmp"
MINIO_FOLDER = "algorithm_aircraft_planificator_outputs" 
SERVER_EXECUTIONS_DIR = "/algoritms/executions/"

# Funcion de conversión de fecha a milisegundos
def to_millis(dt):
    return int(dt.timestamp() * 1000)


def normalize_input_data(input_data):
    # Convertir 'fires[].position' de array [x, y] a dict con srid, x, y, z
    for fire in input_data.get('fires', []):
        if isinstance(fire.get("position"), list):
            x, y = fire["position"]
            fire["position"] = {
                "srid": 4326,
                "x": x,
                "y": y,
                "z": 0
            }

    # Asegurar que 'reserve' tiene un 'model'
    if 'reserve' in input_data and 'model' not in input_data['reserve']:
        all_models = [v['model'] for v in input_data.get('vehicles', []) if 'model' in v]
        input_data['reserve']['model'] = all_models[0] if all_models else "B407"

    return input_data

# Definición de la función para construir el payload para EINFOREX
def build_einforex_payload(fire, vehicles, assignment_criteria):
    now = datetime.utcnow()
    start_dt = now.replace(minute=((now.minute // 10 + 1) * 10) % 60, second=0, microsecond=0)
    end_dt = start_dt + timedelta(hours=4)

    fire_id = fire['id']
    matched_criteria = [c for c in assignment_criteria if c['fireId'] == fire_id]

    available_aircrafts = []
    aircrafts_for_criteria = []
    if matched_criteria:
        for model in matched_criteria[0]['vehicleModels']:
            matched_vehicles = [v for v in vehicles if v['model'] == model]
            ids = [v['id'] for v in matched_vehicles]
            available_aircrafts.extend(ids)
            aircrafts_for_criteria.extend(ids)

    if not available_aircrafts:
        available_aircrafts = []
        aircrafts_for_criteria = [""]

    output_interval_ms = 600_000

    return {
        "startDate": to_millis(start_dt),
        "endDate": to_millis(end_dt),
        "sinceDate": to_millis(start_dt),
        "untilDate": to_millis(end_dt),
        "fireLocation": {
            "srid": fire['position']['srid'],
            "x": fire['position']['x'],
            "y": fire['position']['y'],
            "z": fire['position'].get('z', 0)
        },
        "availableAircrafts": available_aircrafts,
        "outputInterval": output_interval_ms,
        "resourcePlanningCriteria": [
            {
                "since": to_millis(start_dt),
                "until": to_millis(end_dt),
                "aircrafts": aircrafts_for_criteria,
                "waterAmount": None,
                "aircraftNum": len([a for a in aircrafts_for_criteria if a])
            }
        ],
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
    if isinstance(raw_conf, str):
        raw_conf = json.loads(raw_conf)

    message = raw_conf.get('message')
    if not message:
        raise ValueError("Input sin campo 'message'.")

    user = message.get('from_user')
    raw_input_data = message.get('input_data')
    if not raw_input_data:
        raise ValueError("Input sin 'input_data'.")

    input_data = json.loads(raw_input_data) if isinstance(raw_input_data, str) else raw_input_data

    # Normalización del input
    input_data = normalize_input_data(input_data)

    vehicles = input_data['vehicles']
    fires = input_data['fires']
    assignment_criteria = input_data['assignmentCriteria']

    context['ti'].xcom_push(key='user', value=user)

    ssh_conn = BaseHook.get_connection("ssh_avincis_2")
    hostname, username = ssh_conn.host, ssh_conn.login
    ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p-2")).decode("utf-8")

    fire = fires[0]  # Usamos solo el primer fuego
    payload = build_einforex_payload(fire, vehicles, assignment_criteria)
    planning_id = get_planning_id_from_einforex(payload)
    context['ti'].xcom_push(key='planning_id', value=planning_id)

    input_content = f"""medios=a
url1=https://pre.atcservices.cirpas.gal/rest/ResourcePlanningAlgorithmExecutionService/get?id={planning_id}
url2=https://pre.atcservices.cirpas.gal/rest/FlightQueryService/searchByCriteria
url3=https://pre.atcservices.cirpas.gal/rest/FlightReportService/getReport
url4=https://pre.atcservices.cirpas.gal/rest/AircraftStatusService/getAll
url5=https://pre.atcservices.cirpas.gal/rest/AircraftBaseService/getAll
url6=https://pre.atcservices.cirpas.gal/rest/ResourcePlanningAlgorithmExecutionService/update
user=ITMATI.DES
password=Cui_1234
modelos_aeronave=input/modelos_vehiculo.csv
"""

    print(f"[INFO] Input content:\n{input_content}")

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

        with sftp.file(f"{execution_path}/input/{INPUT_FILENAME}", 'w') as remote_file:
            remote_file.write(input_content)

        sftp.close()
        client.close()
        bastion.close()
        print("[INFO] Input preparado y subido correctamente.")
    finally:
        os.remove(temp_key_path)



# Definición de la función para ejecutar el algoritmo y descargar el resultado
def run_and_download_algorithm(**context):
    print("[INFO] Iniciando ejecución de run_and_download_algorithm...")

    ssh_conn = BaseHook.get_connection("ssh_avincis_2")
    hostname, username = ssh_conn.host, ssh_conn.login
    print(f"[INFO] Conectando a bastión {hostname} como {username}")
    ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p-2")).decode("utf-8")

    execution_path = context['ti'].xcom_pull(key='execution_path')
    if not execution_path:
        raise Exception("[ERROR] No se encontró execution_path en XComs")

    input_path = f"{execution_path}/input/{INPUT_FILENAME}"  # Este es el path real del input
    print(f"[INFO] Usando input_path: {input_path}")

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file_key:
        temp_file_key.write(ssh_key_decoded)
        temp_key_path = temp_file_key.name
    os.chmod(temp_key_path, 0o600)

    try:
        bastion = paramiko.SSHClient()
        bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        bastion.connect(hostname=hostname, username=username, key_filename=temp_key_path)
        print("[INFO] Conexión SSH establecida con el bastión")

        jump = bastion.get_transport().open_channel(
            "direct-tcpip", dest_addr=("10.38.9.6", 22), src_addr=("127.0.0.1", 0)
        )
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname="10.38.9.6", username="airflow-executor", sock=jump, key_filename=temp_key_path)
        print("[INFO] Conexión SSH establecida con el servidor interno (10.38.9.6)")

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
        if exit_status == 0:
            print("[INFO] Algoritmo ejecutado exitosamente.")
        else:
            print(f"[ERROR] Algoritmo terminó con error. Exit status: {exit_status}")

        sftp = client.open_sftp()
        print(f"[INFO] Listando ficheros en {SERVER_OUTPUT_DIR}")
        output_files = sftp.listdir(SERVER_OUTPUT_DIR)
        print(f"[INFO] Ficheros encontrados: {output_files}")

        json_filename = next((f for f in output_files if f.endswith('.json')), None)
        if not json_filename:
            raise Exception("[ERROR] No se encontró JSON de salida en el servidor.")

        print(f"[INFO] JSON encontrado: {json_filename}")
        
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp_file:
            sftp.get(SERVER_OUTPUT_DIR + json_filename, tmp_file.name)
            tmp_file.seek(0)
            file_content = tmp_file.read()

        context['ti'].xcom_push(key='json_content', value=file_content)
        context['ti'].xcom_push(key='json_filename', value=json_filename)

        print("[INFO] Output descargado y almacenado en XComs correctamente.")

        sftp.close()
        client.close()
        bastion.close()
        print("[INFO] Conexiones SSH cerradas correctamente.")

    except Exception as e:
        print(f"[ERROR] Error durante la ejecución de run_and_download_algorithm: {e}")
        raise

    finally:
        if os.path.exists(temp_key_path):
            os.remove(temp_key_path)
            print(f"[INFO] Clave SSH temporal eliminada: {temp_key_path}")

# Eliminar carpeta de ejecución

def process_outputs(**context):
    print("[INFO] Iniciando ejecución de process_outputs...")

    json_content = context['ti'].xcom_pull(key='json_content')
    json_filename = context['ti'].xcom_pull(key='json_filename')
    local_payload_json = context['ti'].xcom_pull(key='local_payload_json')

    print(f"[INFO] json_filename: {json_filename}")
    print(f"[INFO] local_payload_json: {local_payload_json}")

    if not json_content:
        raise ValueError("[ERROR] No se encontró el contenido del JSON en XCom")

    s3_client = get_minio_client()
    bucket = MINIO_BUCKET
    folder = MINIO_FOLDER
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    print(f"[INFO] Timestamp para histórico: {timestamp}")

    session = None
    try:
        # Guardar el JSON en un archivo temporal solo para subirlo
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.json') as tmp_file:
            tmp_file.write(json_content)
            tmp_file_path = tmp_file.name

        key_current = f"{folder}/jsons/{json_filename}"
        key_historic = f"{folder}/historic/{timestamp}_{json_filename}"

        print(f"[INFO] Subiendo JSON actual a {bucket}/{key_current}")
        s3_client.upload_file(tmp_file_path, bucket, key_current)
        print(f"[INFO] Subiendo JSON histórico a {bucket}/{key_historic}")
        s3_client.upload_file(tmp_file_path, bucket, key_historic)

        json_url = f"https://minio.avincis.cuatrodigital.com/{bucket}/{key_current}"
        context['ti'].xcom_push(key='json_url', value=json_url)
        print(f"[INFO] JSON subido y url disponible: {json_url}")

        # Generar CSV desde el JSON y subir a MinIO
        csv_filename = json_filename.replace('.json', '.csv')
        local_csv_path = f"/tmp/{csv_filename}"

        output_data = json.loads(json_content)
        csv_data = output_data.get("resourcePlanningResult", [])
        if not csv_data:
            print("[WARN] El JSON no contiene resourcePlanningResult")

        with open(local_csv_path, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["since", "until", "aircrafts"])
            writer.writeheader()
            for row in csv_data:
                writer.writerow({
                    "since": row.get("since"),
                    "until": row.get("until"),
                    "aircrafts": ', '.join(row.get("aircrafts", []))
                })

        csv_key = f"{folder}/outputs/{csv_filename}"
        print(f"[INFO] Subiendo CSV de resultados a {bucket}/{csv_key}")
        s3_client.upload_file(local_csv_path, bucket, csv_key)

        csv_url = f"https://minio.avincis.cuatrodigital.com/{bucket}/{csv_key}"
        context['ti'].xcom_push(key='csv_url', value=csv_url)
        print(f"[INFO] CSV subido y url disponible: {csv_url}")

        # Subir payload JSON si existe
        if local_payload_json and os.path.exists(local_payload_json):
            payload_key = f"{folder}/inputs/{os.path.basename(local_payload_json)}"
            print(f"[INFO] Subiendo payload a {bucket}/{payload_key}")
            s3_client.upload_file(local_payload_json, bucket, payload_key)

            payload_url = f"https://minio.avincis.cuatrodigital.com/{bucket}/{payload_key}"
            context['ti'].xcom_push(key='payload_json_url', value=payload_url)
            print(f"[INFO] Payload JSON subido: {payload_url}")
        else:
            print("[WARN] No se encontró el payload JSON para subir")

        # Guardar en base de datos
        session = get_db_session()
        madrid_tz = pytz.timezone('Europe/Madrid')
        print("[INFO] Insertando resultado en base de datos...")

        session.execute(text("""
            INSERT INTO algoritmos.algoritmo_aircraft_planificator (sampled_feature, result_time, phenomenon_time, input_data, output_data)
            VALUES (:sampled_feature, :result_time, :phenomenon_time, :input_data, :output_data)
        """), {
            "sampled_feature": output_data.get("assignmentId", "unknown"),
            "result_time": datetime.now(madrid_tz),
            "phenomenon_time": datetime.now(madrid_tz),
            "input_data": json.dumps({}, ensure_ascii=False),
            "output_data": json.dumps(output_data, ensure_ascii=False)
        })
        session.commit()
        print("[INFO] Histórico insertado correctamente en base de datos.")

    except Exception as e:
        if session:
            session.rollback()
        print(f"[ERROR] Error durante el proceso de outputs: {e}")
        raise

    finally:
        if session:
            session.close()
            print("[INFO] Sesión de base de datos cerrada correctamente.")
        if 'tmp_file_path' in locals() and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
            print(f"[INFO] Fichero temporal eliminado: {tmp_file_path}")
        if local_payload_json and os.path.exists(local_payload_json):
            os.remove(local_payload_json)
            print(f"[INFO] Fichero temporal local_payload_json eliminado: {local_payload_json}")
        if os.path.exists(local_csv_path):
            os.remove(local_csv_path)
            print(f"[INFO] Fichero CSV temporal eliminado: {local_csv_path}")

    print("[INFO] Finalizada ejecución de process_outputs.")




def fetch_results_from_einforex(**context):
    from requests.auth import HTTPBasicAuth

    print("[INFO] Iniciando fetch_results_from_einforex...")


    # # planning_id = context['ti'].xcom_pull(task_ids='prepare_and_upload_input', key='planning_id')
    # if not planning_id:
    #     raise ValueError("[ERROR] No se encontró planning_id en XCom")
    
    planning_id = 1356

    connection = BaseHook.get_connection('einforex_planning_url')
    url = f"{connection.host}/rest/ResourcePlanningAlgorithmExecutionService/get?id={planning_id}"
    username = connection.login
    password = connection.password

    print(f"[INFO] Llamando a EINFOREX para resultados con ID: {planning_id}")
    print(f"[INFO] URL: {url}")

    try:
        response = requests.get(url, auth=HTTPBasicAuth(username, password), timeout=30)
        response.raise_for_status()
        result_data = response.json()

        print("[INFO] Resultados del algoritmo obtenidos correctamente.")
        print("[INFO] Ejemplo de resultado:")
        print(json.dumps(result_data, indent=2))

        context['ti'].xcom_push(key='einforex_result', value=result_data)

    except Exception as e:
        print(f"[ERROR] Fallo al obtener resultados desde EINFOREX: {e}")
        raise



# Definición de la función para notificar al frontend y guardar en la base de datos

def notify_frontend(**context):
    print("[INFO] Iniciando notificación al frontend...")

    user = context['ti'].xcom_pull(key='user')
    csv_url = context['ti'].xcom_pull(key='csv_url')
    json_url = context['ti'].xcom_pull(key='json_url')

    print(f"[INFO] Usuario destino: {user}")
    print(f"[INFO] CSV URL: {csv_url}")
    print(f"[INFO] JSON URL: {json_url}")

    session = get_db_session()
    now_utc = datetime.now(pytz.utc)

    actions = []

    # Mensaje visual inicial
    actions.append({
        "type": "notify",
        "data": {
            "message": "Resultados del algoritmo disponibles"
        }
    })

    # Tabla CSV si está disponible
    if csv_url:
        actions.append({
            "type": "loadTable",
            "data": {
                "url": csv_url
            }
        })
    else:
        print("[WARN] CSV URL no disponible. No se incluirá loadTable en la notificación.")

    # JSON de resultados
    if json_url:
        actions.append({
            "type": "loadJson",
            "data": {
                "url": json_url,
                "message": "Descargar JSON de resultados."
            }
        })
    else:
        print("[WARN] JSON URL no disponible. No se incluirá loadJson en la notificación.")

    payload = {
        "to": user,
        "actions": actions
    }

    try:
        print("[INFO] Insertando notificación en base de datos...")
        session.execute(text("""
            INSERT INTO public.notifications (destination, "data", "date", status)
            VALUES ('ignis', :data, :date, NULL)
        """), {'data': json.dumps(payload), 'date': now_utc})
        session.commit()
        print("[INFO] Notificación insertada correctamente.")
    except Exception as e:
        session.rollback()
        print(f"[ERROR] Error al insertar la notificación: {e}")
        raise
    finally:
        session.close()
        print("[INFO] Sesión de base de datos cerrada.")

    print("[INFO] Notificación enviada al frontend.")



def always_save_logs(**context):
    print("[INFO] always_save_logs ejecutado.")
    return True

# Definición DAG

default_args = {
    'owner': 'oscar',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'algorithm_aircraft_recommendation',
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

from utils.log_utils import setup_conditional_log_saving

check_logs, save_logs = setup_conditional_log_saving(
    dag=dag,
    task_id='save_logs_to_minio',
    task_id_to_save='run_and_download_algorithm',
    condition_function=always_save_logs
)

# Definir la secuencia de tareas y dependencias

prepare_task >> run_task >> fetch_result_task >> process_task >> notify_task
