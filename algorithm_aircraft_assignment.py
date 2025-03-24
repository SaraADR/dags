from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import json
from airflow.providers.ssh.hooks.ssh import SSHHook
import base64
from airflow.models import Variable
import os

def process_element(**context):
    # print("Algoritmo de asignación de aeronaves")
    # message = context['dag_run'].conf
    # input_data_str = message['message']['input_data']
    # input_data = json.loads(input_data_str)
    # task_type = message['message']['job']
    # from_user = message['message']['from_user']
    # print(input_data)

    # Ruta temporal para almacenar la clave privada en el contenedor
    SSH_KEY_PATH = "/opt/airflow/id_rsa"
    ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p")).decode("utf-8")
    os.makedirs(os.path.dirname(SSH_KEY_PATH), exist_ok=True)
    with open(SSH_KEY_PATH, "w") as f:
        f.write(ssh_key_decoded)
    os.chmod(SSH_KEY_PATH, 0o600)

    ssh_hook = SSHHook(
            ssh_conn_id='ssh_avincis',  # ID de conexión en Airflow
            key_file=SSH_KEY_PATH       # Ruta de la clave temporal
    )
    with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            print("✅ Conexión SSH exitosa")

            # Ejemplo: Listar archivos en el directorio remoto
            remote_files = sftp.listdir('/')
            print("📂 Archivos remotos:", remote_files)

            sftp.close()





default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime.datetime(2025, 1, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

dag = DAG(
    'algorithm_aircraft_assignment',
    default_args=default_args,
    description='Algoritmo de asignación de aeronaves',
    schedule_interval=None, 
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

process_element_task = PythonOperator(
    task_id='process_message',
    python_callable=process_element,
    provide_context=True,
    dag=dag,
)

process_element_task
