import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.hooks.base_hook import BaseHook
import json


def process_element(**context):
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')

    try:
        # Conectarse al servidor SSH
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            print(f"Sftp abierto")


            remote_directory = '/home/admin3/algoritmo-calculo-dNBR/input'
            remote_file_name = 'Test_2_1.json'
            remote_file_path = os.path.join(remote_directory, remote_file_name)
            sftp.chdir(remote_directory)
            print(f"Cambiando al directorio: {remote_directory}")

            output_directory = '/home/admin3/algoritmo-calculo-dNBR/output/test_2_1'
            local_output_directory = '/tmp'

            # Crear el directorio local si no existe
            os.makedirs(local_output_directory, exist_ok=True)
            sftp.chdir(output_directory)
            print(f"Cambiando al directorio de salida: {output_directory}")
    except Exception as e:
        print(f"Error: {str(e)}")    
    return


default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),

}

dag = DAG(
    'algorithm_dNBR_process',
    default_args=default_args,
    description='Algoritmo dNBR',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

# Manda correo
process_element_task = PythonOperator(
    task_id='process_message',
    python_callable=process_element,
    provide_context=True,
    dag=dag,
)


process_element_task 