from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import csv
import base64
import tempfile
import os
import paramiko
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from sqlalchemy import text
from dag_utils import get_minio_client, get_db_session, minio_api, obtener_id_mision
from pytz import timezone

madrid_tz = timezone('Europe/Madrid')

def execute_algorithm_remote(**context):
    print("Inicio de ejecución del algoritmo remoto")

    message = context['dag_run'].conf
    print("Datos recibidos desde el frontend:")
    print(json.dumps(message, indent=2))

    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str) if isinstance(input_data_str, str) else input_data_str
    print("Contenido de input_data (original):")
    print(json.dumps(input_data, indent=2))

    input_data["assignmentId"] = 1356
    if "fires" in input_data:
        for fire in input_data["fires"]:
            fire["level"] = 3

    print("Contenido de input_data (modificado):")
    print(json.dumps(input_data, indent=2))

    user = message['message']['from_user']
    context['ti'].xcom_push(key='user', value=user)

    ssh_conn_bastion = BaseHook.get_connection("ssh_avincis_2")
    hostname = ssh_conn_bastion.host
    username_bastion = ssh_conn_bastion.login
    password_bastion = ssh_conn_bastion.password

    ssh_conn_internal = BaseHook.get_connection("ssh_citmaga_password")
    username_internal = ssh_conn_internal.login
    password_internal = ssh_conn_internal.password

    try:
        print("Estableciendo conexión SSH con bastión")
        bastion = paramiko.SSHClient()
        bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        bastion.connect(hostname=hostname, username=username_bastion, password=password_bastion)

        jump_transport = bastion.get_transport()
        jump_channel = jump_transport.open_channel("direct-tcpip", dest_addr=("10.38.9.6", 22), src_addr=("127.0.0.1", 0))

        print("Conectando al servidor interno como citmaga")
        target_client = paramiko.SSHClient()
        target_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        target_client.connect(hostname="10.38.9.6", username=username_internal, password=password_internal, sock=jump_channel)

        sftp = target_client.open_sftp()

        assignment_id = 1356
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        execution_folder = f"EJECUCION_{assignment_id}_{timestamp}"
        base_path = f"/algoritms/executions/{execution_folder}"
        input_dir = f"{base_path}/input"
        output_dir = f"{base_path}/output"
        input_file = f"{input_dir}/input.json"
        output_file = f"{output_dir}/output.json"

        for path in [base_path, input_dir, output_dir]:
            try:
                sftp.stat(path)
            except FileNotFoundError:
                sftp.mkdir(path)
                print(f"Directorio creado: {path}")

        with sftp.file(input_file, 'w') as remote_file:
            remote_file.write(json.dumps(input_data, indent=2))
        print("Archivo input.json subido al servidor")

        sftp.close()

        cmd = f'python3 /algoritms/algoritmo-asignacion-aeronaves-objetivo-5/call_aircraft_dispatch.py {input_file} {output_file}'
        print("Ejecutando el algoritmo remoto...")
        stdin, stdout, stderr = target_client.exec_command(cmd)
        print("STDOUT:")
        print(stdout.read().decode())
        print("STDERR:")
        print(stderr.read().decode())

        target_client.close()
        bastion.close()
        print("Conexión SSH finalizada correctamente")

    except Exception as e:
        print(f"[ERROR] Fallo en la ejecución remota: {e}")
        raise


def process_output_and_notify(**context):
    user = context['ti'].xcom_pull(key='user')
    message = context['dag_run'].conf.get("message", {})
    input_data_str = message.get("input_data")
    input_data = json.loads(input_data_str) if isinstance(input_data_str, str) else input_data_str
    assignment_id = input_data.get("assignmentId")

    bastion_conn = BaseHook.get_connection("ssh_avincis_2")
    bastion_host = bastion_conn.host
    bastion_user = bastion_conn.login
    ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p-2")).decode("utf-8")

    target_conn = BaseHook.get_connection("ssh_citmaga_password")
    target_user = target_conn.login
    target_pass = target_conn.password

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        temp_file.write(ssh_key_decoded)
        temp_key_path = temp_file.name
    os.chmod(temp_key_path, 0o600)

    local_json_path = f"/tmp/{assignment_id}_output.json"

    try:
        bastion = paramiko.SSHClient()
        bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        bastion.connect(hostname=bastion_host, username=bastion_user, key_filename=temp_key_path)

        jump = bastion.get_transport().open_channel("direct-tcpip", dest_addr=("10.38.9.6", 22), src_addr=("127.0.0.1", 0))
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname="10.38.9.6", username=target_user, password=target_pass, sock=jump)

        sftp = client.open_sftp()
        sftp.get(f"/algoritms/executions/EJECUCION_{assignment_id}/output/output.json", local_json_path)
        sftp.close()
        client.close()
        bastion.close()
    finally:
        os.remove(temp_key_path)

    with open(local_json_path, 'r') as f:
        output_data = json.load(f)

    for a in output_data.get("assignments", []):
        a["missionId"] = obtener_id_mision(a.get("id"))

    csv_path = f"/tmp/{assignment_id}.csv"
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(["fire_id", "aircrafts"])
        for a in output_data.get("assignments", []):
            writer.writerow([a["id"], ", ".join(a.get("vehicles", []))])

    s3 = get_minio_client()
    bucket = "tmp"
    folder = "algorithm_aircraft_planificator_outputs"
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    json_key = f"{folder}/jsons/{assignment_id}_{timestamp}.json"
    csv_key = f"{folder}/outputs/{assignment_id}_{timestamp}.csv"
    s3.upload_file(local_json_path, bucket, json_key)
    s3.upload_file(csv_path, bucket, csv_key)

    base_url = minio_api()
    json_url = f"{base_url}/{bucket}/{json_key}"
    csv_url = f"{base_url}/{bucket}/{csv_key}"

    session = get_db_session()
    now_utc = datetime.utcnow()
    result = session.execute(text("""
        INSERT INTO public.notifications (destination, "data", "date", status)
        VALUES ('ignis', '{}', :date, NULL)
        RETURNING id
    """), {'date': now_utc})
    job_id = result.scalar()

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
                    "title": "Recomendación de aeronaves"
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

    session.execute(text("""
        UPDATE public.notifications SET data = :data WHERE id = :id
    """), {"data": json.dumps(payload, ensure_ascii=False), "id": job_id})
    session.commit()

    try:
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
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()


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