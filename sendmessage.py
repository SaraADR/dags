from datetime import datetime
from airflow import DAG
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'send_test_email',
    default_args=default_args,
    description='A simple DAG to send a test email',
    schedule_interval=None,
    catchup=False,
) as dag:

    send_email = EmailOperator(
        task_id='send_email',
        to='sara_adr@hotmail.com',  # Cambia esto a la dirección de correo del destinatario
        subject='Test Email from Airflow',
        html_content='<p>This is a test email sent from an Airflow DAG!</p>',
    )

    send_email

#stcp fpia fdtb etek