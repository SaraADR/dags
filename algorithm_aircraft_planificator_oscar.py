from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime, timedelta
import paramiko
import os
import tempfile
import base64
import json
from sqlalchemy import text
from dag_utils import get_db_session
import pytz

def insert_notification(payload):
    try:
        session = get_db_session()
        engine = session.get_bind()

        now_utc = datetime.now(pytz.utc)

        query = text("""
            INSERT INTO public.notifications
            (destination, "data", "date", status)
            VALUES (:destination, :data, :date, NULL);
        """)
        session.execute(query, {
            'destination': 'ignis',
            'data': json.dumps(payload, ensure_ascii=False),
            'date': now_utc
        })
        session.commit()

    except Exception as e:
        session.rollback()
        print(f"Error insertando notificación: {str(e)}")
    finally:
        session.close()

def process_fixed_output_from_server():
    output_file = "test1.1.json"  # Fichero fijo que quieres procesar
    user = "Francisco José Blanco Garza"  # Usuario destino

    ssh_conn = BaseHook.get_connection("ssh_avincis_2")
    hostname = ssh_conn.host
    username = ssh_conn.login

    ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p-2")).decode("utf-8")
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        temp_file.write(ssh_key_decoded)
        temp_file_path = temp_file.name
    os.chmod(temp_file_path, 0o600)

    try:
        bastion = paramiko.SSHClient()
        bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        bastion.connect(hostname=hostname, username=username, key_filename=temp_file_path)

        jump_transport = bastion.get_transport()
        jump_channel = jump_transport.open_channel(
            "direct-tcpip",
            dest_addr=("10.38.9.6", 22),
            src_addr=("127.0.0.1", 0)
        )

        target_client = paramiko.SSHClient()
        target_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        target_client.connect(
            hostname="10.38.9.6",
            username="airflow-executor",
            sock=jump_channel,
            key_filename=temp_file_path
        )

        sftp = target_client.open_sftp()

        remote_output_path = f'/algoritms/algoritmo-asignacion-aeronaves-objetivo-5/output/{output_file}'
        local_tmp_output = tempfile.NamedTemporaryFile(delete=False).name

        sftp.get(remote_output_path, local_tmp_output)

        sftp.close()
        target_client.close()
        bastion.close()

    finally:
        os.remove(temp_file_path)

    with open(local_tmp_output, 'r', encoding='utf-8') as f:
        output_data = json.load(f)

    os.remove(local_tmp_output)

    assignment_id = output_data.get('assignmentId')
    assignments_list = output_data.get('assignments', [])

    headers = ["assignment_id", "vehicle_id"]
    rows = []

    for assignment in assignments_list:
        aid = assignment.get('id')
        vehicles = assignment.get('vehicles', [])
        for vehicle in vehicles:
            rows.append([aid, vehicle])

    payload = {
        "to": user,
        "actions": [
            {
                "type": "notify",
                "data": {
                    "message": "Datos del plan procesados correctamente"
                }
            },
            {
                "type": "loadTable",
                "data": {
                    "headers": headers,
                    "rows": rows
                }
            }
        ]
    }

    insert_notification(payload)


default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'algorithm_aircraft_planificator',
    default_args=default_args,
    description='DAG de prueba: descarga test1.1.json de servidor y genera notificación automáticamente',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

process_fixed_output_task = PythonOperator(
    task_id='execute_algorithm_planificator',
    python_callable=process_fixed_output_from_server,
    dag=dag,
)

































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
