from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.email import EmailOperator
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def print_message_and_send_email(**context):
    message = context['dag_run'].conf
    print(f"Received message: {message}")

    data = json.loads(message.get('data', '{}'))  # Decodificar el campo 'data'
    to = data.get('to', 'default@example.com')
    subject = data.get('subject', 'No Subject')
    body = data.get('body', 'No Body')



    email_operator = EmailOperator(
        task_id='send_email_task',
        to=to,
        subject=subject,
        html_content=f'<p>{body}</p>',
        conn_id='smtp_default'
    )
    return email_operator.execute(context)


dag = DAG(
    'recivekafka',
    default_args=default_args,
    description='DAG que imprime el mensaje recibido a través de XCom',
    schedule_interval=None,
    catchup=False
)

print_message_task = PythonOperator(
    task_id='print_message',
    python_callable=print_message_and_send_email,
    provide_context=True,
    dag=dag,
)

print_message_task
