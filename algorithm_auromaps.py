from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import ast
import json

def print_message(**context):
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    menssage_str = message['message']

    input_data = json.loads(input_data_str)
    input_message = json.loads(menssage_str)
    location = input_data['input']['location']
    perimeter = input_data['input']['perimeter']
    from_user = input_message['from_user']
    
    # Imprime las propiedades
    print(f"Location: {location}")
    print(f"Perimeter: {perimeter}")
    print(f"from_user: {from_user}")


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
    'algorithm_automaps',
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


print_message_task