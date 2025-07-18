from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import paramiko
import tempfile
import os

def test_ssh_bastion(**context):
    print("[INFO] Iniciando prueba de conexión SSH al bastión...")

    # Obtener conexión desde Airflow
    ssh_conn = BaseHook.get_connection("ssh_avincis_2")
    hostname = ssh_conn.host
    username = ssh_conn.login

    # Obtener clave SSH desde Variables
    ssh_key = Variable.get("ssh_avincis_p-2")

    # Escribir clave privada en archivo temporal
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as key_file:
        key_file.write(ssh_key)
        key_path = key_file.name
    os.chmod(key_path, 0o600)

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=hostname, username=username, key_filename=key_path)

        stdin, stdout, stderr = ssh.exec_command("hostname && whoami")
        print("[INFO] Resultado del comando remoto:")
        print(stdout.read().decode())
        print(stderr.read().decode())

        ssh.close()
        print("[INFO] Conexión SSH al bastión finalizada correctamente.")
    except Exception as e:
        print(f"[ERROR] Falló la conexión SSH: {e}")
        raise
    finally:
        os.remove(key_path)
        print("[INFO] Archivo de clave temporal eliminado.")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='test_ssh_bastion_connection',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Prueba de conexión SSH al bastión usando paramiko',
    tags=['test', 'ssh']
)

test_ssh = PythonOperator(
    task_id='ssh_connection_test',
    python_callable=test_ssh_bastion,
    dag=dag
)