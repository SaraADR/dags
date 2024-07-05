from datetime import datetime
import json
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 5),
    'email_on_failure': False,
    'email_on_retry': False,
}

def consumer_function(message, prefix=None):
    if message is not None:
        try:
            message_json = json.loads(message.value().decode('utf-8'))
        except json.JSONDecodeError as e:
            print(f'Error decoding JSON: {e}')
            return False

        # Loguear el contenido de message_json
        print(f'Mensaje consumido: {message_json}')
        if message_json.get('destination') == 'email':
            print(f'Mensaje consumido con destination: {message_json.get('destination')}')
            return message_json
    return None

def send_email_function(message_json, **kwargs):
    # Obtener el mensaje desde XComs
    ti = kwargs['ti']

    if not message_json:
        print("No se recibió ningún mensaje.")
        return

    data = json.loads(message_json.get('data', '{}'))
    to = data.get('to', 'default@example.com')
    subject = data.get('subject', 'No Subject')
    body = data.get('body', 'No Body')

    destin = message_json.get('destination', {})
    idd = message_json.get('id', {})
    
    # Loguear el contenido de 'data'
    ti.log.info(f'MensajeJSON: {message_json}')
    ti.log.info(f'Contenido de destin: {destin}')
    ti.log.info(f'Contenido de id: {idd}')
    ti.log.info(f'Contenido de data: {data}')

    email_operator = EmailOperator(
        task_id='send_email_task',
        to=to,
        subject=subject,
        html_content=f'<p>{body}</p>',
        conn_id='smtp_default'
    )
    email_operator.execute(context=kwargs)

with DAG(
    'send_test_email2',
    default_args=default_args,
    description='A DAG to send emails based on Kafka message',
    schedule_interval=None,
    catchup=False,
) as dag:
    
    consume_task = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        topics=["test1"],
        apply_function=consumer_function,
        kafka_config_id="kafka_connection",
        commit_cadence="end_of_batch",
        max_messages=10,
        max_batch_size=2,
    )

    send_email_task = PythonOperator(
        task_id='send_email_task',
        python_callable=send_email_function,
        op_kwargs={'message_json': consume_task.output},
        provide_context=True,
    )

    # Define el operador SQLExecuteQueryOperator para actualizar el estado en la base de datos
    update_status_task = SQLExecuteQueryOperator(
        task_id='update_status_task',
        conn_id='biobd', 
        sql="UPDATE public.notifications SET status = 'ok' WHERE id = '{{ task_instance.xcom_pull(task_ids='consume_from_topic')['id'] }}';",
    )

    consume_task >> send_email_task >> update_status_task
