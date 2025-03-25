from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import json
from airflow.providers.ssh.hooks.ssh import SSHHook
import base64
from airflow.models import Variable
import os
import paramiko
import tempfile

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

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        temp_file.write(ssh_key_decoded)
        temp_file_path = temp_file.name  # Guardar la ruta del archivo temporal

    with open(temp_file_path, 'r') as file:

        jump_host_hook = SSHHook(
            ssh_conn_id='ssh_avincis',  
            key_file=temp_file_path       
        )

        with jump_host_hook.get_conn() as jump_host_client:
            print("✅ Conexión SSH con máquina intermedia exitosa")
            jump_host_client.close()

    #     # Crear cliente Paramiko para el segundo salto
    #     transport = jump_host_client.get_transport()
    #     dest_addr = ('10.38.9.6', 22)  # Dirección IP privada del servidor destino
    #     local_addr = ('127.0.0.1', 0)
        
    #     jump_channel = transport.open_channel("direct-tcpip", dest_addr, local_addr)

    #     second_client = paramiko.SSHClient()
    #     second_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    #     second_client.connect(
    #         '10.38.9.6',
    #         username='usuario_destino',
    #         sock=jump_channel
    #     )
    #     print("✅ Conexión SSH con servidor privado exitosa")

    #     # Ejemplo de listar archivos en el servidor destino
    #     stdin, stdout, stderr = second_client.exec_command('ls /ruta/deseada')
    #     print("📂 Archivos en el servidor destino:", stdout.read().decode())

    #     second_client.close()






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
