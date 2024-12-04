from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from jinja2 import Template
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker

default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def receive_data_and_create_fire(**context):
    message = context['dag_run'].conf
    if not message:
        print("No 'message' field found in the received data.")
        return

    try:
        # Extraemos el campo 'data' que está dentro del 'message'
        data_str = message.get('data')
        if not data_str:
            print("No 'data' field found in the 'message'.")
            return

        # Si el campo 'data' es una cadena, lo decodificamos como JSON (99% veces va a ser)
        data = json.loads(data_str) if isinstance(data_str, str) else data_str
        print(f"Received message data: {data}")
        # aqui es donde se inserta en bd
        # INSERT EN BD CREATE FIRE

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return

    return 


dag = DAG(
    'function_create_fire_from_rabbit',
    default_args=default_args,
    description='DAG que crea el fire desde el rabbit',
    schedule_interval=None,
    catchup=False
)

# Manda correo
receive_data_process = PythonOperator(
    task_id='receive_and_create_fire',
    python_callable=receive_data_and_create_fire,
    provide_context=True,
    dag=dag,
)

receive_data_process 
#>> other_task_vacia