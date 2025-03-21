from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import json


def process_element(**context):
    print("Algoritmo de asignación de aeronaves")
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str)
    task_type = message['message']['job']
    from_user = message['message']['from_user']
    print(input_data)

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
    'algorithm_aircraft_assignment',
    default_args=default_args,
    description='Algoritmo de asignación de aeronaves',
    schedule_interval='@daily', 
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
