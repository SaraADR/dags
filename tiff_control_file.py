from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import ast


def print_message(**context):
    message = context['dag_run'].conf
    print(f"Received message: {message}")
    message_dict = ast.literal_eval(message['message'])
    print(f"Received message: {message_dict}")


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

# Manda correo
print_message_task = PythonOperator(
    task_id='print_message',
    python_callable=print_message,
    provide_context=True,
    dag=dag,
)


print_message_task