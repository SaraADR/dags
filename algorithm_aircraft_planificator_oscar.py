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
import paramiko
import pytz
from sqlalchemy import text
from dag_utils import get_minio_client, get_db_session


def prepare_and_upload_input(**context):
    message = context['dag_run'].conf['message']
    input_data = message['input_data']
    user = message['from_user']

    # Mock: obtener planning_id (cuando tengas API real, aquí haces el POST)
    planning_id = 12345
    print(f"[API] Planning ID obtenido: {planning_id}")

    context['ti'].xcom_push(key='planning_id', value=planning_id)
    context['ti'].xcom_push(key='user', value=user)

    ssh_conn = BaseHook.get_connection("ssh_avincis_2")
    hostname, username = ssh_conn.host, ssh_conn.login
    ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p-2")).decode("utf-8")

    url_servicio = "https://api.einforex.gal/servicio-planificacion"
    modelos_vehiculo_path = "/algoritms/algoritmo-asignacion-aeronaves-objetivo-5/input/modelos_vehiculo.csv"
    input_txt_content = f"{url_servicio}\n{planning_id}\n{modelos_vehiculo_path}\n"

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        temp_file.write(ssh_key_decoded)
        temp_file_path = temp_file.name
    os.chmod(temp_file_path, 0o600)

    try:
        bastion = paramiko.SSHClient()
        bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        bastion.connect(hostname=hostname, username=username, key_filename=temp_file_path)

        jump = bastion.get_transport().open_channel("direct-tcpip", dest_addr=("10.38.9.6", 22), src_addr=("127.0.0.1", 0))
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname="10.38.9.6", username="airflow-executor", sock=jump, key_filename=temp_file_path)

        sftp = client.open_sftp()
        with sftp.file('/algoritms/algoritmo-asignacion-aeronaves-objetivo-5/input/input_data_aeronaves.txt', 'w') as remote_file:
            remote_file.write(input_txt_content)

        sftp.close()
        client.close()
        bastion.close()
        print("[UPLOAD] Input generado y subido.")
    finally:
        os.remove(temp_file_path)

def run_and_download_algorithm(**context):
    ssh_conn = BaseHook.get_connection("ssh_avincis_2")
    hostname, username = ssh_conn.host, ssh_conn.login
    ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p-2")).decode("utf-8")

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        temp_file.write(ssh_key_decoded)
        temp_file_path = temp_file.name
    os.chmod(temp_file_path, 0o600)

    try:
        bastion = paramiko.SSHClient()
        bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        bastion.connect(hostname=hostname, username=username, key_filename=temp_file_path)

        jump = bastion.get_transport().open_channel("direct-tcpip", dest_addr=("10.38.9.6", 22), src_addr=("127.0.0.1", 0))
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname="10.38.9.6", username="airflow-executor", sock=jump, key_filename=temp_file_path)

        # Ejecutar el algoritmo
        cmd = 'cd /algoritms/algoritmo-asignacion-aeronaves-objetivo-5 && python3 call_recomendador.py input/input_data_aeronaves.txt'
        stdin, stdout, stderr = client.exec_command(cmd)
        print(stdout.read().decode())
        print(stderr.read().decode())

        # Descargar el JSON
        sftp = client.open_sftp()
        output_dir = '/algoritms/algoritmo-asignacion-aeronaves-objetivo-5/output/'
        output_files = sftp.listdir(output_dir)

        json_filename = next((f for f in output_files if f.endswith('.json')), None)
        if not json_filename:
            raise Exception("No se encontró un archivo .json de salida.")

        local_tmp_output = tempfile.NamedTemporaryFile(delete=False).name
        sftp.get(output_dir + json_filename, local_tmp_output)

        context['ti'].xcom_push(key='json_filename', value=json_filename)
        context['ti'].xcom_push(key='local_tmp_output', value=local_tmp_output)

        sftp.close()
        client.close()
        bastion.close()
    finally:
        os.remove(temp_file_path)


def process_outputs(**context):
    json_filename = context['ti'].xcom_pull(key='json_filename')
    local_tmp_output = context['ti'].xcom_pull(key='local_tmp_output')

    s3_client = get_minio_client()
    bucket_name = "tmp"
    folder = "algorithm_aircraft_planificator_outputs"
    minio_base_url = "https://minio.avincis.cuatrodigital.com/"

    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    key_current = f"{folder}/jsons/{json_filename}"
    key_historic = f"{folder}/historic/{timestamp}_{json_filename}"

    s3_client.upload_file(local_tmp_output, bucket_name, key_current)
    s3_client.upload_file(local_tmp_output, bucket_name, key_historic)

    json_url = f"{minio_base_url}/{bucket_name}/{key_current}"
    context['ti'].xcom_push(key='json_url', value=json_url)

    # Crear y subir CSV
    with open(local_tmp_output, 'r', encoding='utf-8') as f:
        output_data = json.load(f)

    local_csv_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
    with open(local_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['assignment_id', 'vehicle_id'])
        for assignment in output_data.get('assignments', []):
            for vehicle in assignment.get('vehicles', []):
                writer.writerow([assignment['id'], vehicle])

    csv_key = f"{folder}/{os.path.basename(local_csv_file)}"
    s3_client.upload_file(local_csv_file, bucket_name, csv_key)

    csv_url = f"{minio_base_url}/{bucket_name}/{csv_key}"
    context['ti'].xcom_push(key='csv_url', value=csv_url)

    # Insertar histórico en BD
    session = get_db_session()
    madrid_tz = pytz.timezone('Europe/Madrid')
    try:
        datos = {
            "sampled_feature": output_data.get("assignmentId", "unknown"),
            "result_time": datetime.now(madrid_tz),
            "phenomenon_time": datetime.now(madrid_tz),
            "input_data": json.dumps({}),
            "output_data": json.dumps(output_data)
        }
        session.execute(text("""
            INSERT INTO algoritmos.algoritmo_aircraft_planificator (sampled_feature, result_time, phenomenon_time, input_data, output_data)
            VALUES (:sampled_feature, :result_time, :phenomenon_time, :input_data, :output_data)
        """), datos)
        session.commit()
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()

    os.remove(local_tmp_output)
    os.remove(local_csv_file)

def notify_frontend(**context):
    user = context['ti'].xcom_pull(key='user')
    csv_url = context['ti'].xcom_pull(key='csv_url')
    json_url = context['ti'].xcom_pull(key='json_url')

    session = get_db_session()
    try:
        now_utc = datetime.now(pytz.utc)
        payload = {
            "to": user,
            "actions": [
                {"type": "load_csv_table", "data": {"message": "Resultados disponibles."}},
                {"type": "loadTable", "data": {"url": csv_url}},
                {"type": "loadJson", "data": {"url": json_url, "message": "Descargar JSON de resultados."}}
            ]
        }
        session.execute(text("""
            INSERT INTO public.notifications (destination, "data", "date", status)
            VALUES ('ignis', :data, :date, NULL)
        """), {'data': json.dumps(payload), 'date': now_utc})
        session.commit()
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()

default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'algorithm_aircraft_planificator',
    default_args=default_args,
    description='DAG Planificador aeronaves - compacto en 4 procesos',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

prepare_and_upload_input_task = PythonOperator(
    task_id='prepare_and_upload_input',
    python_callable=prepare_and_upload_input,
    provide_context=True,
    dag=dag,
)

run_and_download_algorithm_task = PythonOperator(
    task_id='run_and_download_algorithm',
    python_callable=run_and_download_algorithm,
    provide_context=True,
    dag=dag,
)

process_outputs_task = PythonOperator(
    task_id='process_outputs',
    python_callable=process_outputs,
    provide_context=True,
    dag=dag,
)

notify_frontend_task = PythonOperator(
    task_id='notify_frontend',
    python_callable=notify_frontend,
    provide_context=True,
    dag=dag,
)

# Encadenamiento
prepare_and_upload_input_task >> run_and_download_algorithm_task >> process_outputs_task >> notify_frontend_task






































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
