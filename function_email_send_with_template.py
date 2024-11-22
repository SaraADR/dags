from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
import json
from jinja2 import Template
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker


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

def render_template(message_dict, templateId):
    print('dict ' + message_dict)
    email_data = {}

    # Añadir aqui todas las posibilidades de plantillas. Si no hay ninguna plantilla se usará por defecto la 1
    if templateId == '1':
        with open(PLANTILLA_1) as file:
            template_str = file.read()
            jinja_template = Template(template_str)
        email_data = {
            'dato': message_dict,
        }
    elif templateId == '2':
        with open(PLANTILLA_2) as file:
            template_str = file.read()
            jinja_template = Template(template_str)
        email_data = {
            'dato': message_dict,
        }
    elif templateId == '3':
        with open(PLANTILLA_3) as file:
            template_str = file.read()
            jinja_template = Template(template_str)
        email_data = {
            'dato':  message_dict,
        }

    email_content = jinja_template.render(email_data)
    return email_content

def print_message_and_send_email(**context):
    message = context['dag_run'].conf
    print(f"Received message: {message}")

    inner_message = message.get('message')
    if not inner_message:
        print("No 'message' field found in the received data.")
        return

    try:
        # Extraemos el campo 'data' que está dentro del 'message'
        data_str = inner_message.get('data')
        if not data_str:
            print("No 'data' field found in the 'message'.")
            return

        # Si el campo 'data' es una cadena, lo decodificamos como JSON
        data = json.loads(data_str) if isinstance(data_str, str) else data_str
        print(f"Received message data: {data}")

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return

    # Guardamos el id para poder hacer la modificación posterior en la base de datos
    context['ti'].xcom_push(key='message_id', value=message.get('id'))



    to = data.get('to', 'default@example.com')
    cc = data.get('cc', 'default@example.com')  # Extracting CC field
    bcc = data.get('bcc', 'default@example.com')  # Extracting BCC field
    subject = data.get('subject', 'No Subject')
    email_body = render_template(data.get('body', ''), data.get('templateId', ''))

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


def change_state_noti(**context):
    message = context['dag_run'].conf
    print(f"Received message: {message}")
    
    inner_message = message.get('message')
    if not inner_message:
        print("No 'message' field found in the received data.")
        return

    idElemento = inner_message.get('id')
    try:
   
        # Conexión a la base de datos usando las credenciales almacenadas en Airflow
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

        
        # Update job status to 'FINISHED'
        metadata = MetaData(bind=engine)
        notificationTable = Table('notifications', metadata, schema='public', autoload_with=engine)
        update_stmt = notificationTable.update().where(notificationTable.c.id == idElemento).values(status='FINISHED')
        session.execute(update_stmt)
        session.commit()
        print(f"Notificacion ID {idElemento} status updated to FINISHED")

    except Exception as e:
        session.rollback()
        print(f"Error durante el guardado del estado de la notificacion: {str(e)}")



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

#Cambia estado de job
change_state_task = PythonOperator(
    task_id='change_state_job',
    python_callable=change_state_noti,
    provide_context=True,
    dag=dag,
)


print_message_task >> change_state_task
