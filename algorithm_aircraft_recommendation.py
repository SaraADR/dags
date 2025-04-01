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
    input_data = context['dag_run'].conf.get('inputData', {})
    mission_data = input_data.get('missionData', [])

    print("InputData recibido desde ignis:")
    print(json.dumps(input_data, indent=2))

    try:
        # 1. Obtener conexión y clave SSH desde Airflow
        ssh_conn = BaseHook.get_connection("ssh_avincis")
        hostname = ssh_conn.host
        username = ssh_conn.login

        ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p")).decode("utf-8")

        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
            temp_file.write(ssh_key_decoded)
            temp_file_path = temp_file.name
        os.chmod(temp_file_path, 0o600)

        # 2. Conectarse al bastión (máquina intermedia)
        jump_host = paramiko.SSHClient()
        jump_host.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        jump_host.connect(
            hostname=hostname,
            username=username,
            key_filename=temp_file_path
        )
        print("Conexión SSH con máquina intermedia (bastión) exitosa")

        # 3. Saltar al servidor interno
        transport = jump_host.get_transport()
        jump_channel = transport.open_channel(
            "direct-tcpip",
            dest_addr=("10.38.9.6", 22),
            src_addr=("127.0.0.1", 0)
        )

        second_client = paramiko.SSHClient()
        second_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        second_client.connect(
            hostname="10.38.9.6",
            username="usuario_destino",  # ← REEMPLAZAR si no coincide con el anterior
            sock=jump_channel
        )
        print("Conexión SSH con servidor Avincis exitosa")

    #     # 4. Comando de prueba
    #     stdin, stdout, stderr = second_client.exec_command('ls -l ~/algoritmo')
    #     print("Archivos en ~/algoritmo:")
    #     print(stdout.read().decode())

    #     # 5. Ejecutar el algoritmo remoto
    #     cmd = 'cd ~/algoritmo && source venv/bin/activate && python call_recomendador.py input/input_data_aeronaves.txt'
    #     stdin, stdout, stderr = second_client.exec_command(cmd)

    #     print("STDOUT del algoritmo:")
    #     print(stdout.read().decode())

    #     print("STDERR del algoritmo:")
    #     print(stderr.read().decode())

    #     second_client.close()
    #     jump_host.close()

    finally:
        # Limpiar el archivo temporal de la clave SSH
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
    'algorithm_aircraft_recommendation',
    default_args=default_args,
    description='Lanzamiento remoto del algoritmo de recomendación de medios aéreos en Avincis',
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

process_element_task
