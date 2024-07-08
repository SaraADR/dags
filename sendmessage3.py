from datetime import datetime
import json
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Variable global para almacenar el mensaje consumido
message_json = {}
otro_json = {}


def send_email_function(**kwargs):

    message = kwargs['dag_run'].conf
    if message:
        # Aquí puedes procesar el mensaje JSON como desees
        print(f"Received message: {message}")

    # global message_json
    # data = message_json.get('data', {})
    # to = data.get('to', 'default@example.com')
    # subject = data.get('subject', 'No Subject')
    # body = data.get('body', 'No Body')

    # destin = message_json.get('destination', {})
    # idd = message_json.get('id', {})
    
    # # Loguear el contenido de 'data'
    # kwargs['ti'].log.info(f'Mensaje: {otro_json}')
    # kwargs['ti'].log.info(f'MensajeJSON: {message_json}')
    # kwargs['ti'].log.info(f'Contenido de destin: {destin}')
    # kwargs['ti'].log.info(f'Contenido de id: {idd}')
    # kwargs['ti'].log.info(f'Contenido de data: {data}')

    # email_operator = EmailOperator(
    #     task_id='send_email_task',
    #     to=to,
    #     subject=subject,
    #     html_content=f'<p>{body}</p>',
    #     conn_id='smtp_default'
    # )
    return 
    #return email_operator.execute(context=kwargs)


with DAG(
    'sendmessage3',
    default_args=default_args,
    description='DAG',
    schedule_interval=None,
    catchup=False,
) as dag:
    
    send_email_task = PythonOperator(
        task_id='send_email_task',
        python_callable=send_email_function,
        provide_context=True,
    )

    send_email_task

