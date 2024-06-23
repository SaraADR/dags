from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'email_notification_dag',
    default_args=default_args,
    description='Email notification',
    schedule_interval=None,
)

def create_email_task(dag, to, subject, body, cc, bcc, attachments):
    return EmailOperator(
        task_id='send_email',
        to=to,
        subject=subject,
        html_content=body,
        cc=cc,
        bcc=bcc,
        #files=attachments,
        dag=dag,
    )

# Valores de ejemplo; reemplazar con los valores reales
to = 'sara.arrdr@gmail.com'
subject = 'Test Email'
body = 'This is a test email'
cc = ['sara_adr@hotmail.com']
bcc = ['sara.arrdr@gmail.com']
#attachments = ['/path/to/attachment.txt']

send_email_task = create_email_task(dag, to, subject, body, cc, bcc, attachments)
