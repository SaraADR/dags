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
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str) if isinstance(input_data_str, str) else input_data_str

    input_data["assignmentId"] = 1356
    for fire in input_data.get("fires", []):
        fire["level"] = 3

    user = message['message']['from_user']
    context['ti'].xcom_push(key='user', value=user)

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

    try:
        bastion = paramiko.SSHClient()
        bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        bastion.connect(hostname=bastion_host, username=bastion_user, key_filename=temp_key_path)

        jump = bastion.get_transport().open_channel("direct-tcpip", dest_addr=("10.38.9.6", 22), src_addr=("127.0.0.1", 0))
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname="10.38.9.6", username=target_user, password=target_pass, sock=jump)

        sftp = client.open_sftp()
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        base_path = f"/algoritms/executions/EJECUCION_1356_{timestamp}"
        input_file = f"{base_path}/input/input.json"
        output_file = f"{base_path}/output/output.json"

        for path in [base_path, f"{base_path}/input", f"{base_path}/output"]:
            try:
                sftp.stat(path)
            except FileNotFoundError:
                sftp.mkdir(path)

        with sftp.file(input_file, 'w') as f:
            f.write(json.dumps(input_data, indent=2))
        sftp.close()

        cmd = f"python3 /algoritms/algoritmo-asignacion-aeronaves-objetivo-5/call_aircraft_dispatch.py {input_file} {output_file}"
        stdin, stdout, stderr = client.exec_command(cmd)
        print(stdout.read().decode())
        print(stderr.read().decode())

        client.close()
        bastion.close()
    finally:
        os.remove(temp_key_path)


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