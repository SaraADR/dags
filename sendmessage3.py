from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def process_message(**kwargs):
    message = kwargs['dag_run'].conf
    if message:
        # Aquí puedes procesar el mensaje JSON como desees
        print(f"Received message: {message}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sendmessage3',
    default_args=default_args,
    description='DAG to process JSON message',
    schedule_interval=None,  # Este DAG se dispara desde otro DAG, no necesita schedule
    catchup=False,
)

process_message_task = PythonOperator(
    task_id='process_message',
    provide_context=True,
    python_callable=process_message,
    dag=dag,
)
