import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.hooks.base_hook import BaseHook
import json
import pytz
from airflow.models import Variable
from dag_utils import execute_query

def process_element(**context):

    madrid_tz = pytz.timezone('Europe/Madrid')
    fechaHoraActual = datetime.datetime.now(madrid_tz)  # Fecha y hora con zona horaria

    print(f"Este algoritmo se está ejecutando a las {fechaHoraActual.strftime('%Y-%m-%d %H:%M:%S')} en Madrid, España")

    tipo1diasincendio = Variable.get("dNBR_diasFinIncendio", default_var="10")
    print(f"Valor de la variable tipo1diasincendio en Airflow: {tipo1diasincendio}")

    tipo2mesesminimo = Variable.get("dNBR_mesesFinIncendioMinimo", default_var="3")
    print(f"Valor de la variable tipo2mesesminimo en Airflow: {tipo2mesesminimo}")

    tipo2mesesmaximo = Variable.get("dNBR_mesesFinIncendioMaximo", default_var="10000")
    print(f"Valor de la variable tipo2mesesmaximo en Airflow: {tipo2mesesmaximo}")

    query = """
        SELECT m.id , mf.fire_id, m.start_date , m.end_date
        FROM public.mss_mission m
        JOIN public.mss_mission_fire mf ON m.id = mf.mission_id
        WHERE m.end_date <= NOW() - INTERVAL ':days days'
    """
    params = {'days': tipo1diasincendio}
    result = execute_query('biobd', query, params)
    print(result)


    # ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')

    # try:
    #     # Conectarse al servidor SSH
    #     with ssh_hook.get_conn() as ssh_client:
    #         sftp = ssh_client.open_sftp()
    #         print(f"Sftp abierto")


    #         remote_directory = '/home/admin3/algoritmo-calculo-dNBR/input'
    #         remote_file_name = 'Test_2_1.json'
    #         remote_file_path = os.path.join(remote_directory, remote_file_name)
    #         sftp.chdir(remote_directory)
    #         print(f"Cambiando al directorio: {remote_directory}")

    #         output_directory = '/home/admin3/algoritmo-calculo-dNBR/output/test_2_1'
    #         local_output_directory = '/tmp'

    #         # Crear el directorio local si no existe
    #         os.makedirs(local_output_directory, exist_ok=True)
    #         sftp.chdir(output_directory)
    #         print(f"Cambiando al directorio de salida: {output_directory}")
    #         sftp.close()
    # except Exception as e:
    #     print(f"Error: {str(e)}")    
    #     sftp.close()
    # return


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
    schedule_interval='*/2 * * * *',
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