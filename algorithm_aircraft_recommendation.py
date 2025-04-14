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

    vehicles = input_data['vehicles']
    fires = input_data['fires']
    assignment_criteria = input_data['assignmentCriteria']

    context['ti'].xcom_push(key='user', value=user)

    ssh_conn = BaseHook.get_connection("ssh_avincis_2")
    hostname, username = ssh_conn.host, ssh_conn.login
    ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p-2")).decode("utf-8")

    # SOLO VAMOS A GENERAR 1 PlanningID (del primer fuego)
    fire = fires[0]
    payload = build_einforex_payload(fire, vehicles, assignment_criteria)
    planning_id = get_planning_id_from_einforex(payload)

    # Ahora montamos el input_data_aeronaves.txt
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
    
    print(f"Informacion del input data enviado")
    print(f"[INFO] Input content: {input_content}")


    # Crear carpeta de ejecución
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

        # Subir input
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

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file_key:
        temp_file_key.write(ssh_key_decoded)
        temp_key_path = temp_file_key.name
    os.chmod(temp_key_path, 0o600)
    print(f"[INFO] Clave SSH temporal generada en {temp_key_path}")

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

        cmd = 'cd /algoritms/algoritmo-recomendador-objetivo-5 && python3 call_recomendador.py input/input_data_aeronaves.txt'
        print(f"[INFO] Ejecutando comando remoto: {cmd}")
        stdin, stdout, stderr = client.exec_command(cmd)

        # Esperamos a que el comando finalice y mostramos la salida y errores
        exit_status = stdout.channel.recv_exit_status()
        if exit_status == 0:
            print("[INFO] Algoritmo ejecutado exitosamente")
        else:
            print(f"[ERROR] Algoritmo terminó con error. Exit status: {exit_status}")
            for line in stderr.readlines():
                print(f"[ERROR OUTPUT] {line.strip()}")

        sftp = client.open_sftp()
        print(f"[INFO] Listando ficheros en {SERVER_OUTPUT_DIR}")
        output_files = sftp.listdir(SERVER_OUTPUT_DIR)
        print(f"[INFO] Ficheros encontrados: {output_files}")

        json_filename = next((f for f in output_files if f.endswith('.json')), None)
        if not json_filename:
            raise Exception("[ERROR] No se encontró JSON de salida en el servidor.")

        print(f"[INFO] JSON encontrado: {json_filename}")
        local_tmp_output = tempfile.NamedTemporaryFile(delete=False).name
        print(f"[INFO] Descargando {json_filename} a {local_tmp_output}")
        sftp.get(SERVER_OUTPUT_DIR + json_filename, local_tmp_output)

        context['ti'].xcom_push(key='json_filename', value=json_filename)
        context['ti'].xcom_push(key='local_tmp_output', value=local_tmp_output)
        print("[INFO] Output descargado y almacenado en XComs correctamente")

        sftp.close()
        client.close()
        bastion.close()
        print("[INFO] Conexiones SSH cerradas correctamente")

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

    local_tmp_output = context['ti'].xcom_pull(key='local_tmp_output')
    json_filename = context['ti'].xcom_pull(key='json_filename')
    local_payload_json = context['ti'].xcom_pull(key='local_payload_json')

    print(f"[INFO] local_tmp_output: {local_tmp_output}")
    print(f"[INFO] json_filename: {json_filename}")
    print(f"[INFO] local_payload_json: {local_payload_json}")

    s3_client = get_minio_client()
    bucket = MINIO_BUCKET
    folder = MINIO_FOLDER
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    print(f"[INFO] Timestamp para histórico: {timestamp}")

    try:
        # Subir JSON de resultados
        key_current = f"{folder}/jsons/{json_filename}"
        key_historic = f"{folder}/historic/{timestamp}_{json_filename}"

        print(f"[INFO] Subiendo JSON actual a {bucket}/{key_current}")
        s3_client.upload_file(local_tmp_output, bucket, key_current)
        print(f"[INFO] Subiendo JSON histórico a {bucket}/{key_historic}")
        s3_client.upload_file(local_tmp_output, bucket, key_historic)

        json_url = f"https://minio.avincis.cuatrodigital.com/{bucket}/{key_current}"
        context['ti'].xcom_push(key='json_url', value=json_url)
        print(f"[INFO] JSON subido y url disponible: {json_url}")

        # Subir payload JSON enviado a EINFOREX
        payload_key = f"{folder}/inputs/{os.path.basename(local_payload_json)}"
        print(f"[INFO] Subiendo payload a {bucket}/{payload_key}")
        s3_client.upload_file(local_payload_json, bucket, payload_key)

        payload_url = f"https://minio.avincis.cuatrodigital.com/{bucket}/{payload_key}"
        context['ti'].xcom_push(key='payload_json_url', value=payload_url)
        print(f"[INFO] Payload JSON subido: {payload_url}")

        # Guardar en base de datos
        session = get_db_session()
        madrid_tz = pytz.timezone('Europe/Madrid')
        print("[INFO] Insertando resultado en base de datos...")

        with open(local_tmp_output, 'r', encoding='utf-8') as f:
            output_data = json.load(f)

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
        session.rollback()
        print(f"[ERROR] Error durante el proceso de outputs: {e}")
        raise

    finally:
        session.close()
        print("[INFO] Sesión de base de datos cerrada correctamente.")

        # Eliminar ficheros temporales locales
        if os.path.exists(local_tmp_output):
            os.remove(local_tmp_output)
            print(f"[INFO] Fichero temporal local_tmp_output eliminado: {local_tmp_output}")

        if os.path.exists(local_payload_json):
            os.remove(local_payload_json)
            print(f"[INFO] Fichero temporal local_payload_json eliminado: {local_payload_json}")

    print("[INFO] Finalizada ejecución de process_outputs")


# Definición de la función para notificar al frontend y guardar en la base de datos

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

# Definir la secuencia de tareas y dependencias

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
