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
from sqlalchemy import text
def process_element(**context):

    madrid_tz = pytz.timezone('Europe/Madrid')
    fechaHoraActual = datetime.datetime.now(madrid_tz)  # Fecha y hora con zona horaria

    print(f"Este algoritmo se está ejecutando a las {fechaHoraActual.strftime('%Y-%m-%d %H:%M:%S')} en Madrid, España")

    tipo1diasincendio = Variable.get("dNBR_diasFinIncendio", default_var="10")
    print(f"Valor de la variable tipo1diasincendio en Airflow: {tipo1diasincendio}")

    interval_value = f'{tipo1diasincendio} days'
    query = f"""
        SELECT m.id, mf.fire_id, m.start_date, m.end_date
        FROM missions.mss_mission m
        JOIN missions.mss_mission_fire mf ON m.id = mf.mission_id
        WHERE mf.extinguishing_timestamp::DATE = (CURRENT_DATE - INTERVAL '{interval_value}')
    """

    result = execute_query('biobd', query)
    print(result)


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
    'algorithm_dNBR_process_Type1',
    default_args=default_args,
    description='Algoritmo dNBR Type 1',
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