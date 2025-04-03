import json
import os
import base64
import tempfile
import paramiko
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.ssh.hooks.ssh import SSHHook

def process_element(**context):
    raw_conf = context['dag_run'].conf
    if not raw_conf:
        raise ValueError("No se recibió mensaje en dag_run.conf")

    message = raw_conf[0] if isinstance(raw_conf, list) else raw_conf
    input_data_str = message.get("input_data", "")
    if not input_data_str:
        raise ValueError("input_data no proporcionado")

    try:
        input_data = json.loads(input_data_str)
    except json.JSONDecodeError:
        raise ValueError("input_data no es un JSON válido")

    assignment_id = input_data.get("assignmentId")
    if not assignment_id:
        raise ValueError("Falta assignmentId")

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

    ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p")).decode("utf-8")
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        temp_file.write(ssh_key_decoded)
        temp_file_path = temp_file.name
    os.chmod(temp_file_path, 0o600)

    try:
        jump_host_hook = SSHHook(
            ssh_conn_id='ssh_avincis',
            key_file=temp_file_path
        )

        with jump_host_hook.get_conn() as jump_host_client:
            transport = jump_host_client.get_transport()
            dest_addr = ('10.38.9.6', 22)
            local_addr = ('127.0.0.1', 0)

            jump_channel = transport.open_channel("direct-tcpip", dest_addr, local_addr)

            second_client = paramiko.SSHClient()
            second_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            second_client.connect(
                '10.38.9.6',
                username='airflow-executor',
                sock=jump_channel,
                key_filename=temp_file_path
            )

            sftp = second_client.open_sftp()
            remote_tmp_path = "/tmp/input_data_aeronaves.txt"
            final_input_path = "/algoritms/algoritmo-recomendador-objetivo-5/Input/input_data_aeronaves.txt"

            with sftp.file(remote_tmp_path, "w") as f:
                f.write(input_txt)

            sftp.close()

            cmd = (
                f"cd /algoritms/algoritmo-recomendador-objetivo-5 && "
                f"mv {remote_tmp_path} {final_input_path} && "
                f"source venv/bin/activate && "
                f"python call_recomendador.py Input/input_data_aeronaves.txt"
            )

            stdin, stdout, stderr = second_client.exec_command(cmd)
            print(stdout.read().decode())
            print(stderr.read().decode())

            second_client.close()

    finally:
        os.remove(temp_file_path)


# DAG config
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
