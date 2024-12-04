from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from jinja2 import Template
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker

default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def print_message_consolelog(**context):
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

    return 


def other_task_esta_vacia(**context):
    return
    # message = context['dag_run'].conf
    # print(f"Received message: {message}")
    
    # inner_message = message.get('message')
    # if not inner_message:
    #     print("No 'message' field found in the received data.")
    #     return

    # idElemento = inner_message.get('id')
    # try:
   
    #     # Conexión a la base de datos usando las credenciales almacenadas en Airflow
    #     db_conn = BaseHook.get_connection('biobd')
    #     connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
    #     engine = create_engine(connection_string)
    #     Session = sessionmaker(bind=engine)
    #     session = Session()

        
    #     # Update job status to 'FINISHED'
    #     metadata = MetaData(bind=engine)
    #     notificationTable = Table('notifications', metadata, schema='public', autoload_with=engine)
    #     update_stmt = notificationTable.update().where(notificationTable.c.id == idElemento).values(status='FINISHED')
    #     session.execute(update_stmt)
    #     session.commit()
    #     print(f"Notificacion ID {idElemento} status updated to FINISHED")

    # except Exception as e:
    #     session.rollback()
    #     print(f"Error durante el guardado del estado de la notificacion: {str(e)}")



dag = DAG(
    'function_create_fire_from_rabbit',
    default_args=default_args,
    description='DAG que crea el fire desde el rabbit',
    schedule_interval=None,
    catchup=False
)

# Manda correo
print_message_clog = PythonOperator(
    task_id='print_message',
    python_callable=print_message_consolelog,
    provide_context=True,
    dag=dag,
)

#Cambia estado de job
other_task_vacia = PythonOperator(
    task_id='change_state_job',
    python_callable=other_task_esta_vacia,
    provide_context=True,
    dag=dag,
)


print_message_clog >> other_task_vacia
