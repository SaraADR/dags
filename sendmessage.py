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

def consumer_function(message, prefix=None):
    if message is not None:
        global message_json
        message_json = json.loads(message.value().decode('utf-8'))
        # Loguear el contenido de message_json
        if message_json.get('destination') == 'email':
            return True
    return False

def send_email_function(**kwargs):
    global message_json
    data = message_json.get('data', {})
    to = data.get('to', 'default@example.com')
    subject = data.get('subject', 'No Subject')
    body = data.get('body', 'No Body')

    destin = message_json.get('destination', {})
    idd = message_json.get('id', {})
    # Loguear el contenido de 'data'
    kwargs['ti'].log.info(f'Contenido de destin: {destin}')
    kwargs['ti'].log.info(f'Contenido de id: {idd}')
    kwargs['ti'].log.info(f'Contenido de data: {data}')

    email_operator = EmailOperator(
        task_id='send_email_task',
        to=to,
        subject=subject,
        html_content=f'<p>{body}</p>',
        conn_id='smtp_default'
    )
    return email_operator.execute(context=kwargs)



with DAG(
    'send_test_email',
    default_args=default_args,
    description='A DAG to send emails based on Kafka message',
    schedule_interval=None,
    catchup=False,
) as dag:
    
    consume_task = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        topics=["test1"],
        apply_function=consumer_function,
        apply_function_kwargs={"prefix": "consumed:::"},
        kafka_config_id="kafka_connection",
        commit_cadence="end_of_batch",
        max_messages=10,
        max_batch_size=2,
    )


    # send_email = EmailOperator(
    #     task_id='send_email',
    #     to='sara_adr@hotmail.com',  # Cambia esto a la dirección de correo del destinatario
    #     subject='Test Email from Airflow',
    #     html_content='<p>This is a test email sent from an Airflow DAG!</p>',
    #     conn_id='smtp_default' 
    # )

    send_email_task = PythonOperator(
        task_id='send_email_task',
        python_callable=send_email_function,
        provide_context=True,
    )


    # Define el operador Postgres para actualizar el estado en la base de datos
    update_status_task = PostgresOperator(
        task_id='update_status_task',
        postgres_conn_id='biobd', 
        sql="UPDATE public.notifications SET status = 'ok' WHERE id = {{ ti.xcom_pull(task_ids='consume_from_topic')['id'] }};",
    )

    consume_task >> send_email_task >> update_status_task

#stcp fpia fdtb etek