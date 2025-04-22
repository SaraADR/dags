from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import csv
import base64
import tempfile
import os
from io import BytesIO
import paramiko
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from sqlalchemy import text
from dag_utils import get_minio_client, get_db_session, minio_api, obtener_id_mision
from pytz import timezone
madrid_tz = timezone('Europe/Madrid')
now = datetime.now(madrid_tz)
#VERSION PARA PRUEBAS CON JSON METIDO A PIÑON


def execute_algorithm_remote(**context):
    print("[INFO] Inicio de ejecución del algoritmo remoto")

    message = context['dag_run'].conf
    print("[INFO] Datos recibidos desde el frontend:")
    print(json.dumps(message, indent=2))

    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str) if isinstance(input_data_str, str) else input_data_str
    print("[INFO] Contenido de input_data:")
    print(json.dumps(input_data, indent=2))

    user = message['message']['from_user']
    context['ti'].xcom_push(key='user', value=user)

    assignment_id = input_data.get("assignmentId")
    print(f"[INFO] assignment_id: {assignment_id}")


def process_output_and_notify(**context):

    print("Procesando output y notificando al frontend")
    message = context['dag_run'].conf.get('message', {})
    input_data_str = message.get('input_data')
    input_data = json.loads(input_data_str) if isinstance(input_data_str, str) else input_data_str
    assignment_id = input_data['assignmentId']
    user = context['ti'].xcom_pull(key='user')
    print(f"[INFO] assignment_id: {assignment_id}")

    s3_client = get_minio_client()
    bucket = "tmp"
    test_json_key = "algorithm_aircraft_planificator_outputs/historic/output_test4.2.json"
    print(f"[INFO] Descargando JSON de prueba desde MinIO: {test_json_key}")

    response = s3_client.get_object(Bucket=bucket, Key=test_json_key)
    output_data = json.load(response['Body'])

    # Añadir missionId a cada assignment
    for assignment in output_data.get("assignments", []):
        fire_id = assignment.get("id")
        mission_id = obtener_id_mision(fire_id) if fire_id else None
        assignment["missionId"] = mission_id

    print(f"[INFO] JSON descargado desde MinIO")
    print(f"[DEBUG] Claves del JSON descargado: {list(output_data.keys())}")
    print(f"[DEBUG] Contenido 'assignments': {output_data.get('assignments')}")

    csv_data = output_data.get("assignments", [])
    print(f"[DEBUG] Preparando escritura de CSV con {len(csv_data)} filas")

    csv_filename = f"{assignment_id}.csv"
    csv_local_path = f"/tmp/{csv_filename}"

    # Extraer primer fire_id para convertir a mission_id
    fire_id = csv_data[0].get("id") if csv_data else None
    mission_id = obtener_id_mision(fire_id) if fire_id else None
    print(f"[INFO] Misión ID obtenida: {mission_id}")

    with open(csv_local_path, 'w', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(["fire_id", "aircrafts"])
        for row in csv_data:
            fire = row.get("id")
            aircrafts = ", ".join(row.get("vehicles", []))
            writer.writerow([fire, aircrafts])

    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    json_key = f"algorithm_aircraft_planificator_outputs/jsons/{assignment_id}_{timestamp}.json"
    csv_key = f"algorithm_aircraft_planificator_outputs/outputs/{assignment_id}_{timestamp}.csv"

    output_data["missionId"] = mission_id

    s3_client.put_object(
        Bucket=bucket,
        Key=json_key,
        Body=BytesIO(json.dumps(output_data).encode("utf-8")),
        ContentLength=len(json.dumps(output_data).encode("utf-8"))
    )
    s3_client.upload_file(csv_local_path, bucket, csv_key)

    base_url = minio_api()
    json_url = f"{base_url}/{bucket}/{json_key}"
    csv_url = f"{base_url}/{bucket}/{csv_key}"

    print(f"[INFO] Archivos subidos a MinIO: \n- JSON: {json_url}\n- CSV: {csv_url}")

    session = get_db_session()
    now_utc = datetime.utcnow()
    result = session.execute(text("""
        INSERT INTO public.notifications (destination, "data", "date", status)
        VALUES ('ignis', '{}', :date, NULL)
        RETURNING id
    """), {'date': now_utc})
    job_id = result.scalar()
    print(f"[INFO] Notificación registrada con ID: {job_id}")

    payload = {
        "to": user,
        "actions": [
            {
                "type": "loadTable",
                "data": {
                    "url": csv_url,
                    "button": {
                        "key": "openPlanner",
                        "data": json_url
                    },
                    "title": "Recomendación de aeronaves",
                    "missionId": mission_id
                }
            },
            {
                "type": "notify",
                "data": {
                    "message": f"Resultados del algoritmo disponibles. ID: {job_id}"
                }
            }
        ]
    }

    print(f"[DEBUG] Payload de notificación: {json.dumps(payload, indent=2, ensure_ascii=False)}")

    session.execute(text("""
        UPDATE public.notifications SET data = :data WHERE id = :id
    """), {"data": json.dumps(payload, ensure_ascii=False), "id": job_id})

    try:
        print("[INFO] Guardando resultado en algoritmos.algoritmo_aircraft_recomendador...")

        session.execute(text("""
            INSERT INTO algoritmos.algoritmo_aircraft_recomendador (sampled_feature, result_time, phenomenon_time, input_data, output_data)
            VALUES (:sampled_feature, :result_time, :phenomenon_time, :input_data, :output_data)
        """), {
            "sampled_feature": output_data.get("assignmentId", "unknown"),
            "result_time": datetime.now(madrid_tz),
            "phenomenon_time": datetime.now(madrid_tz),
            "input_data": json.dumps({}, ensure_ascii=False),
            "output_data": json.dumps(output_data, ensure_ascii=False)
        })
        session.commit()
        print("[INFO] Resultado insertado correctamente en la tabla.")
    except Exception as e:
        session.rollback()
        print(f"[ERROR] Error al guardar en la base de datos: {e}")
        raise

    session.commit()
    session.close()
    print(f"[INFO] Notificación actualizada y enviada")




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
    'algorithm_aircraft_recommendation',
    default_args=default_args,
    description='Ejecuta algoritmo de recomendación en servidor Avincis',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

execute_task = PythonOperator(
    task_id='execute_assignation_algorithm',
    python_callable=execute_algorithm_remote,
    dag=dag
)

process_task = PythonOperator(
    task_id='process_output_and_notify',
    python_callable=process_output_and_notify,
    dag=dag
)

execute_task >> process_task



#VERSION CUANDO EL ALGORITMO FUNCIONE CORRECTAMENTE

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
# from sqlalchemy import text
# import csv
# from dag_utils import get_minio_client, get_db_session

# def execute_algorithm_remote(**context):
#     print("Inicio de ejecución del algoritmo remoto")

#     message = context['dag_run'].conf
#     print("Datos recibidos desde el frontend:")
#     print(json.dumps(message, indent=2))  # Muestra la estructura JSON de entrada

#     input_data_str = message['message']['input_data']
#     input_data = json.loads(input_data_str) if isinstance(input_data_str, str) else input_data_str
#     print("Contenido de input_data:")
#     print(json.dumps(input_data, indent=2))

#     user = message['message']['from_user']
#     context['ti'].xcom_push(key='user', value=user)

#     ssh_conn = BaseHook.get_connection("ssh_avincis_2")
#     hostname = ssh_conn.host
#     username = ssh_conn.login

#     ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p-2")).decode("utf-8")
#     with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
#         temp_file.write(ssh_key_decoded)
#         temp_file_path = temp_file.name
#     os.chmod(temp_file_path, 0o600)

#     try:
#         print("Estableciendo conexión SSH con bastión")
#         bastion = paramiko.SSHClient()
#         bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#         bastion.connect(hostname=hostname, username=username, key_filename=temp_file_path)

#         jump_transport = bastion.get_transport()
#         jump_channel = jump_transport.open_channel("direct-tcpip", dest_addr=("10.38.9.6", 22), src_addr=("127.0.0.1", 0))

#         print("Conectando al servidor interno")
#         target_client = paramiko.SSHClient()
#         target_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#         target_client.connect(hostname="10.38.9.6", username="airflow-executor", sock=jump_channel, key_filename=temp_file_path)

#         sftp = target_client.open_sftp()

#         assignment_id = input_data.get("assignmentId")
#         base_path = f"/algoritms/executions/EJECUCION_{assignment_id}"
#         input_dir = f"{base_path}/input"
#         output_dir = f"{base_path}/output"
#         input_file = f"{input_dir}/input.json"
#         output_file = f"{output_dir}/output.json"

#         for path in [base_path, input_dir, output_dir]:
#             try:
#                 sftp.stat(path)
#             except FileNotFoundError:
#                 sftp.mkdir(path)
#                 print(f"Directorio creado: {path}")

#         with sftp.file(input_file, 'w') as remote_file:
#             remote_file.write(json.dumps(input_data, indent=2))
#         print("Archivo input.json subido al servidor")

#         sftp.close()

#         cmd = (
#             f'python3 /algoritms/algoritmo-asignacion-aeronaves-objetivo-5/call_aircraft_dispatch.py '
#             f'{input_file} {output_file}'
#         )
#         print("Ejecutando el algoritmo remoto")
#         stdin, stdout, stderr = target_client.exec_command(cmd)
#         print(stdout.read().decode())
#         print(stderr.read().decode())

#         target_client.close()
#         bastion.close()
#         print("Conexión SSH finalizada")

#     finally:
#         os.remove(temp_file_path)
#         print("Archivo de clave SSH temporal eliminado")

# def process_output_and_notify(**context):
#     from_user = context['ti'].xcom_pull(key='user')
#     message = context['dag_run'].conf.get("message", {})
#     input_data_str = message.get("input_data")

#     # Parsear el string JSON correctamente
#     input_data = json.loads(input_data_str) if isinstance(input_data_str, str) else input_data_str
#     assignment_id = input_data.get("assignmentId")

#     print("[INFO] Procesando output y notificando al frontend")
#     print(f"[INFO] assignment_id: {assignment_id}")
#     user = context['ti'].xcom_pull(key='user')

#     json_filename = "output.json"
#     json_path = f"/algoritms/executions/EJECUCION_{assignment_id}/output/{json_filename}"
#     local_json_path = f"/tmp/{assignment_id}_output.json"

#     ssh_conn = BaseHook.get_connection("ssh_avincis_2")
#     hostname = ssh_conn.host
#     username = ssh_conn.login
#     ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p-2")).decode("utf-8")

#     with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
#         temp_file.write(ssh_key_decoded)
#         temp_key_path = temp_file.name
#     os.chmod(temp_key_path, 0o600)

#     try:
#         bastion = paramiko.SSHClient()
#         bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#         bastion.connect(hostname=hostname, username=username, key_filename=temp_key_path)

#         jump = bastion.get_transport().open_channel("direct-tcpip", dest_addr=("10.38.9.6", 22), src_addr=("127.0.0.1", 0))
#         client = paramiko.SSHClient()
#         client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#         client.connect(hostname="10.38.9.6", username="airflow-executor", sock=jump, key_filename=temp_key_path)

#         sftp = client.open_sftp()
#         sftp.get(json_path, local_json_path)
#         print("Output JSON descargado desde el servidor")
#         sftp.close()
#         client.close()
#         bastion.close()
#     finally:
#         os.remove(temp_key_path)

#     with open(local_json_path, 'r') as f:
#         output_data = json.load(f)

#     csv_data = output_data.get("resourcePlanningResult", [])
#     csv_filename = f"{assignment_id}.csv"
#     csv_local_path = f"/tmp/{csv_filename}"
#     with open(csv_local_path, 'w', newline='') as f:
#         writer = csv.DictWriter(f, fieldnames=["since", "until", "aircrafts"])
#         writer.writeheader()
#         for row in csv_data:
#             writer.writerow({
#                 "since": row.get("since"),
#                 "until": row.get("until"),
#                 "aircrafts": ", ".join(row.get("aircrafts", []))
#             })
#     print("CSV generado a partir del JSON")

#     s3_client = get_minio_client()
#     bucket = "tmp"
#     folder = "algorithm_aircraft_planificator_outputs"
#     timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
#     json_key = f"{folder}/jsons/{assignment_id}_{timestamp}.json"
#     csv_key = f"{folder}/outputs/{assignment_id}_{timestamp}.csv"
#     s3_client.upload_file(local_json_path, bucket, json_key)
#     s3_client.upload_file(csv_local_path, bucket, csv_key)
#     print("Archivos subidos a MinIO")

#     json_url = f"https://minio.avincis.cuatrodigital.com/{bucket}/{json_key}"
#     csv_url = f"https://minio.avincis.cuatrodigital.com/{bucket}/{csv_key}"

#     session = get_db_session()
#     now_utc = datetime.utcnow()
#     result = session.execute(text("""
#         INSERT INTO public.notifications (destination, "data", "date", status)
#         VALUES ('ignis', '{}', :date, NULL)
#         RETURNING id
#     """), {'date': now_utc})
#     job_id = result.scalar()
#     print(f"Notificación registrada en base de datos con ID: {job_id}")

#     payload = {
#         "to": user,
#         "actions": [
#             {
#                 "type": "loadTable",
#                 "data": {
#                     "url": csv_url
#                 }
#             },
#             {
#                 "type": "paintCSV",
#                 "data": {
#                     "url": csv_url,
#                     "action": {
#                         "key": "openPlanner",
#                         "data": json_url
#                     },
#                     "title": "Abrir planner"
#                 }
#             },
#             {
#                 "type": "notify",
#                 "data": {
#                     "message": f"Resultados del algoritmo disponibles. ID: {job_id}"
#                 }
#             }
#         ]
#     }

#     session.execute(text("""
#         UPDATE public.notifications SET data = :data WHERE id = :id
#     """), {"data": json.dumps(payload, ensure_ascii=False), "id": job_id})
#     session.commit()
#     session.close()
#     print("Notificación enviada al frontend y proceso finalizado")

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
#     'algorithm_aircraft_recommendation',
#     default_args=default_args,
#     description='Ejecuta algoritmo de recomendación en servidor Avincis',
#     schedule_interval=None,
#     catchup=False,
#     max_active_runs=1,
#     concurrency=1
# )

# execute_task = PythonOperator(
#     task_id='execute_assignation_algorithm',
#     python_callable=execute_algorithm_remote,
#     dag=dag
# )

# process_task = PythonOperator(
#     task_id='process_output_and_notify',
#     python_callable=process_output_and_notify,
#     dag=dag
# )

# execute_task >> process_task
