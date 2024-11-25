
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import json

default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def prepare_notification(**kwargs):
    # Extraer el mensaje y destino de los parámetros
    message = kwargs.get('message', 'Job created')
    destination = kwargs.get('destination', 'ignis')

    # Crear un diccionario con la notificación
    notification = {
        "type": "job_created",
        "message": message,
        "destination": destination
    }
    # Convertirlo a JSON para almacenarlo
    return json.dumps(notification)

with DAG(
    
    'function_database_notification_insert_and_notify',
    default_args=default_args,
    description='A DAG for sending notifications to the database',
    schedule_interval=None,
    catchup=False,
    
) as dag:

    prepare_notification_task = PythonOperator(
        task_id='prepare_notification',
        python_callable=prepare_notification,
        provide_context=True
    )

    send_notification_task = PostgresOperator(
        task_id='send_notification',
        postgres_conn_id='biobd',
        sql="""
        INSERT INTO public.notifications (destination, data)
        VALUES ('ignis', '{{ task_instance.xcom_pull(task_ids='prepare_notification') }}')
        """,
)

    prepare_notification_task >> send_notification_task
