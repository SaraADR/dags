from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import base64
import tempfile
import os
import paramiko
from airflow.models import Variable
from airflow.hooks.base import BaseHook

def execute_algorithm_remote(**context):
    raw_conf = context['dag_run'].conf

    if not raw_conf:
        raise ValueError("No se recibió mensaje en dag_run.conf")

    try:
        full_message = raw_conf[0] if isinstance(raw_conf, list) else raw_conf
        input_data_raw = full_message.get("input_data", "")

        if not input_data_raw:
            raise ValueError("input_data no proporcionado")

        try:
            input_data_dict = json.loads(input_data_raw)
        except json.JSONDecodeError:
            raise ValueError("input_data no es un JSON válido")

        assignment_id = input_data_dict.get("assignmentId", "")
        if not assignment_id:
            raise ValueError("Falta assignmentId en input_data")

        input_txt = (
            f"medios=a\n"
            f"url1=https://pre.atcservices.cirpas.gal/rest/ResourcePlanningAlgorithmExecutionService/get?id={assignment_id}\n"
            f"url2=https://pre.atcservices.cirpas.gal/rest/FlightQueryService/searchByCriteria\n"
            f"url3=https://pre.atcservices.cirpas.gal/rest/FlightReportService/getReport\n"
            f"url4=https://pre.atcservices.cirpas.gal/rest/AircraftStatusService/getAll\n"
            f"url5=https://pre.atcservices.cirpas.gal/rest/AircraftBaseService/getAll\n"
            f"url6=https://pre.atcservices.cirpas.gal/rest/ResourcePlanningAlgorithmExecutionService/update\n"
            f"user=ITMATI.DES\n"
            f"password=Cui_1234\n"
            f"modelos_aeronave=input/modelos_vehiculo.csv"
        )

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
            temp_remote_path = '/tmp/input_data_aeronaves.txt'
            final_input_path = '/algoritms/algoritmo-recomendador-objetivo-5/Input/input_data_aeronaves.txt'

            with sftp.file(temp_remote_path, 'w') as remote_file:
                remote_file.write(input_txt)
            sftp.close()

            cmd = (
                f'cd /algoritms/algoritmo-recomendador-objetivo-5 && '
                f'mv {temp_remote_path} {final_input_path} && '
                'source venv/bin/activate && '
                'python call_recomendador.py Input/input_data_aeronaves.txt'
            )

            stdin, stdout, stderr = target_client.exec_command(cmd)
            print(stdout.read().decode())
            print(stderr.read().decode())

            target_client.close()
            bastion.close()

        finally:
            os.remove(temp_file_path)

    except Exception as e:
        raise RuntimeError(f"Error en la ejecución: {str(e)}")

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
    description='Ejecuta algoritmo de planificación en servidor Avincis',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

process_element_task = PythonOperator(
    task_id='execute_algorithm_via_jump_host',
    python_callable=execute_algorithm_remote,
    provide_context=True,
    dag=dag,
)
