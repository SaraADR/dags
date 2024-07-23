from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
import ast


def print_message(**context):
    message = context['dag_run'].conf
    print(f"Received message: {message}")



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'tiff_control',
    default_args=default_args,
    description='DAG que controla los tiff llegados a Airflow',
    schedule_interval=None,
    catchup=False
)

# Task para imprimir el mensaje
print_message_task = PythonOperator(
    task_id='print_message',
    python_callable=print_message,
    provide_context=True,
    dag=dag,
)

# Task para ejecutar el comando Docker de forma remota
run_docker_task = SSHOperator(
    task_id='run_docker',
    ssh_conn_id='ssh_docker',  # El ID de la conexión SSH configurada en Airflow
    command='docker run --rm -v /servicios/exiftool:/images --name exiftool-container-new exiftool-image -config /images/example1.1.0_missionId.txt -u /images/img-20230924140747117-ter.tiff',
    dag=dag,
)

# Definir la secuencia de ejecución de las tareas
print_message_task >> run_docker_task
