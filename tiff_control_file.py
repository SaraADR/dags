from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
import ast


def print_message(**context):
    message = context['dag_run'].conf
    print(f"Received message: {message}")


def process_metadata(**kwargs):
    ti = kwargs['ti']
    metadata = ti.xcom_pull(task_ids='run_docker')
    print(f"Metadata received: {metadata}")
    # Aplicar la función de análisis
    metadata_dict = parse_metadata(metadata)
  
    # Imprimir los metadatos en formato JSON
    print(f"Metadata received:\n{json.dumps(metadata_dict, indent=4)}")

def parse_metadata(metadata):
    data = {}
        # Cada línea representa una clave-valor en el formato "Clave : Valor"
    for line in metadata.splitlines():
        if ' : ' in line:
            key, value = line.split(' : ', 1)
            data[key.strip()] = value.strip()
    return data



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

print_message_task = PythonOperator(
    task_id='print_message',
    python_callable=print_message,
    provide_context=True,
    dag=dag,
)

run_docker_task = SSHOperator(
    task_id='run_docker',
    ssh_conn_id='ssh_docker',
    command='docker run --rm -v /servicios/exiftool:/images --name exiftool-container-new exiftool-image -config /images/example1.1.0_missionId.txt -u /images/img-20230924140747117-ter.tiff',
    dag=dag,
    do_xcom_push=True,
)


process_metadata_task = PythonOperator(
    task_id='process_metadata',
    python_callable=process_metadata,
    provide_context=True,
    dag=dag,
)

print_message_task >> run_docker_task >> process_metadata_task
