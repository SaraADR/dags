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
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    
    if isinstance(input_data_str, str):
        input_data = json.loads(input_data_str)
    else:
        input_data = input_data_str
    print(input_data)

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

        # Generar el fichero .properties para aeronaves
        remote_input_path = '/algoritms/algoritmo-recomendador-objetivo-5/Input/input_data_aeronaves.txt'
        
        properties_lines = [
            "medios=a",
            f"url1={input_data['url1']}",
            f"url2={input_data['url2']}",
            f"url3={input_data['url3']}",
            f"url4={input_data['url4']}",
            f"url5={input_data['url5']}",
            f"url6={input_data['url6']}",
            f"user={input_data['user']}",
            f"password={input_data['password']}",
            "modelos_aeronave=input/modelos_vehiculo.csv"
        ]
        
        txt_content = "\n".join(properties_lines)

        with sftp.file(remote_input_path, 'w') as remote_file:
            remote_file.write(txt_content)

        sftp.close()

        cmd = (
            'cd /algoritms/algoritmo-recomendador-objetivo-5 && '
            'python3 call_recomendador.py Input/input_data_aeronaves.txt'
        )

        stdin, stdout, stderr = target_client.exec_command(cmd)
        print(stdout.read().decode())
        print(stderr.read().decode())

        target_client.close()
        bastion.close()

    finally:
        os.remove(temp_file_path)

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
    description='Ejecuta algoritmo de planificación de aeronaves en servidor Avincis',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

process_element_task = PythonOperator(
    task_id='execute_algorithm_planificator',
    python_callable=execute_algorithm_remote,
    dag=dag,
)
