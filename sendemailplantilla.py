from datetime import datetime, timedelta
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.email import EmailOperator
import json
from airflow.providers.postgres.operators.postgres import PostgresOperator
import ast
import os
from jinja2 import Template

PLANTILLA_1 = './dags/repo/recursos/plantillaid1.html'
PLANTILLA_2 = './dags/repo/recursos/plantillaid2.html'
PLANTILLA_3 = './dags/repo/recursos/plantillaid3.html'
LOGO = './dags/repo/recursos/dummy.jpg'

default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def render_template(message_dict):
    data = json.loads(message_dict.get('data', '{}'))
    templateId = data.get('templateId', '3')
    email_data = {}

    # Añadir aqui todas las posibilidades de plantillas. Si no hay ninguna plantilla se usará por defecto la 1
    if templateId == '1':
        with open(PLANTILLA_1) as file:
            template_str = file.read()
            jinja_template = Template(template_str)
        email_data = {
            'nombre': data.get('to', 'default@example.com'),
            'dato1': data.get('subject', 'No Subject'),
            'dato2': data.get('subject', 'No Subject'),
        }
    elif templateId == '2':
        with open(PLANTILLA_2) as file:
            template_str = file.read()
            jinja_template = Template(template_str)
        email_data = {
            'nombre': data.get('to', 'default@example.com'),
            'dato1': data.get('DATO', 'DATO'),
            'dato2': data.get('templateId', 'No Subject'),
        }
    elif templateId == '3':
        with open(PLANTILLA_3) as file:
            template_str = file.read()
            jinja_template = Template(template_str)
        email_data = {
            'nombre': data.get('to', 'default@example.com'),
            'subject': data.get('subject', 'No subject'),
            'dato2': data.get('dato2', 'No Subject'),
            'cc' : data.get('cc', 'default@example.com'),  # Extracting CC field
            'bcc' : data.get('bcc', 'default@example.com')
        }

    email_content = jinja_template.render(email_data)
    return email_content

def print_message_and_send_email(**context):
    message = context['dag_run'].conf
    print(f"Received message: {message}")

    message_dict = ast.literal_eval(message['message'])
    email_body = render_template(message_dict)

    # Guardamos el id para poder hacer la modificación posterior en la base de datos
    context['ti'].xcom_push(key='message_id', value=message_dict.get('id'))

    # Extraemos el campo data
    data = json.loads(message_dict.get('data', '{}'))
    print(f"Received message: {data}")

    to = data.get('to', 'default@example.com')
    cc = data.get('cc', 'default@example.com')  # Extracting CC field
    bcc = data.get('bcc', 'default@example.com')  # Extracting BCC field
    subject = data.get('subject', 'No Subject')

    email_operator = EmailOperator(
        task_id='send_email_task',
        to=to,
        cc=cc,  # Adding CC recipients
        bcc=bcc,  # Adding BCC recipients
        subject=subject,
        html_content=email_body,
        conn_id='test_mailing',
        mime_subtype='related',
        files=[LOGO]
    )
    return email_operator.execute(context)

dag = DAG(
    'send_email_plantilla',
    default_args=default_args,
    description='DAG que envia emails',
    schedule_interval=None,
    catchup=False
)

# Manda correo
print_message_task = PythonOperator(
    task_id='print_message',
    python_callable=print_message_and_send_email,
    provide_context=True,
    dag=dag,
)

# Actualiza bd
update_status_task = PostgresOperator(
    task_id='update_status',
    postgres_conn_id='biobd',  
    sql="""
        UPDATE public.notifications
        SET status = 'ok'
        WHERE id = '{{ ti.xcom_pull(task_ids="print_message", key="message_id") }}';
    """,
    dag=dag,
)

print_message_task >> update_status_task
