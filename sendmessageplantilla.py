from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.email import EmailOperator
import json
from airflow.providers.postgres.operators.postgres import PostgresOperator
import ast
import os
from jinja2 import Template

DAGS_FOLDER = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_PATH = os.path.join(DAGS_FOLDER, 'repo', 'recursos', 'plantillacorreo.html')
LOGO_PATH = os.path.join(DAGS_FOLDER, 'repo', 'recursos', 'dummy.jpg')



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def render_template():

    current_dir = os.getcwd()
    print(f"Current working directory: {current_dir}")
    
    # Listar el contenido del directorio actual
    print("Content of current directory:")
    print(os.listdir(current_dir + '/dags'))
    
    # Verificar y listar el contenido del directorio de recursos
    recursos_dir = os.path.join(current_dir, 'dags','repo', 'recursos')
    print(f"Content of recursos directory ({recursos_dir}):")
    if os.path.exists(recursos_dir):
        print(os.listdir(recursos_dir))
    else:
        print(f"Directory {recursos_dir} does not exist.")



    # Verificar si los archivos existen
    print(f"TEMPLATE_PATH: {DAGS_FOLDER}, exists: {os.path.exists(DAGS_FOLDER)}")
    print(os.listdir(DAGS_FOLDER))


    # data = json.loads(message_dict.get('data', '{}'))

    # print(f"Template Path: {TEMPLATE_PATH}")
    # print(f"Logo path: {LOGO_PATH}")

    with open('./dags/repo/recursos/plantillacorreo.html') as file_:
        template = Template(file_.read())

    # t = Template('Hello, {{ name }}!')
    # print(t.render(name='John Doe'))

    with open('./dags/repo/recursos/plantillacorreo.html') as file:
        template_str = file.read()
    jinja_template = Template(template_str)

    # email_data = {
    #     'nombre': data.get('to', 'default@example.com'),
    #     'dato1': data.get('subject', 'No Subject'),
    #     'dato2': data.get('subject', 'No Subject')
    # }

    email_data = {
            'nombre': 'default@example.com',
            'dato1': 'subject',
            'dato2': 'No Subject'
        }
    email_content = jinja_template.render(email_data)

    return email_content



def print_message_and_send_email(**context):

    # message = context['dag_run'].conf
    # print(f"Received message: {message}")

    

    #message_dict = ast.literal_eval(message['message'])
    email_body = render_template()
    print(email_body)


    # context['ti'].xcom_push(key='message_id', value=message_dict.get('id'))
    # data = json.loads(message_dict.get('data', '{}'))  
    # print(f"Received message: {data}")
    # to = data.get('to', 'default@example.com')
    # subject = data.get('subject', 'No Subject')
    # body = data.get('body', 'No Body')



    # email_operator = EmailOperator(
    #     task_id='send_email_task',
    #     to=to,
    #     subject=subject,
    #     html_content=f'<p>{email_body}</p>',
    #     conn_id='smtp_default',
    #     mime_subtype='related',
    #     files=LOGO_PATH
    # )
    
    # return email_operator.execute(context)


dag = DAG(
    'send_email_wplantilla',
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

update_status_task = PostgresOperator(
    task_id='update_status',
    postgres_conn_id='biobd',  # Asegúrate de que esta conexión esté configurada en Airflow
    sql="""
        UPDATE public.notifications
        SET status = 'ok'
        WHERE id = '{{ ti.xcom_pull(task_ids="print_message", key="message_id") }}';
    """,
    dag=dag,
)

print_message_task >> update_status_task
