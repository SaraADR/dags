from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import paramiko
import base64
import tempfile
import os

def test_ssh_connection():
    # Leer la conexión SSH desde Airflow
    ssh_conn = BaseHook.get_connection("ssh_avincis_2")
    hostname = ssh_conn.host
    username = ssh_conn.login

    # Leer la clave privada desde la variable codificada en base64
    ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p-2")).decode("utf-8")

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        temp_file.write(ssh_key_decoded)
        temp_file_path = temp_file.name
    os.chmod(temp_file_path, 0o600)

    try:
        # Crear cliente SSH y conectar al bastión
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            hostname=hostname,
            username=username,
            key_filename=temp_file_path
        )
        print("Conexión SSH exitosa con el bastión")

        # Ejecutar un comando de prueba
        stdin, stdout, stderr = ssh_client.exec_command("ls -l")
        print("Archivos:")
        print(stdout.read().decode())
        print("Errores (si hay):")
        print(stderr.read().decode())

        ssh_client.close()
    finally:
        os.remove(temp_file_path)

default_args = {
    'owner': 'oscar',
    'start_date': datetime(2025, 1, 1),
    'retries': 0,
}

dag = DAG(
    'test_ssh_connection_avincis_2',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

run_test = PythonOperator(
    task_id='test_ssh',
    python_callable=test_ssh_connection,
    dag=dag,
)
