from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import ast


def print_message(**context):
    message = context['dag_run'].conf
    print(f"Received message: {message}")

def create_fire(**context):
    message = context['dag_run'].conf
    print(f"Received message: {message}")
    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str)
    print(input_data)
    print(input_data['fire'])




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
    'create_fire',
    default_args=default_args,
    description='DAG que envia emails',
    schedule_interval=None,
    catchup=False
)

# Manda correo
print_message_task = PythonOperator(
    task_id='print_message',
    python_callable=print_message,
    provide_context=True,
    dag=dag,
)

atc_create_fire_task = PythonOperator(
    task_id='atc_create_fire',
    python_callable=create_fire,
    provide_context=True,
    dag=dag,
)


print_message_task >> atc_create_fire_task