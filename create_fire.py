from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import ast
import json
import requests
from requests.auth import HTTPBasicAuth
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.orm import sessionmaker

def print_message(**context):
    message = context['dag_run'].conf
    print(f"Received message: {message}")

def create_fire(**context):
    message = context['dag_run'].conf
    print(f"Received message: {message}")
    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str)
    print(input_data)
    print(input_data['fire'])
    print(input_data['type_id'])


    if(input_data['type_id'] == 3):
        #LLAMAR AL ATC
        print("Es de tipo incendios") 
        conn = BaseHook.get_connection('atc_services_connection') 
        auth = (conn.login, conn.password)
        # URL del servicio
        url = f"{conn.host}/rest/FireService/save"
        response = requests.post(url, json=input_data['fire'], auth=auth)
        if response.status_code == 200:
            print("Solicitud exitosa:")
            print(response.json())

            create_mission(response, input_data_str)
        else:
            print(f"Error en la solicitud: {response.status_code}")
            print(response.text)
    else:
        #POR AHORA NADA
        print("No es de tipo incendios")    


def create_mission(fire, job):
    print(fire)
    db_conn = BaseHook.get_connection('biobd')
    connection_string = f"{db_conn.conn_type}://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/missions"
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()


    mapping = {
        'name': 'name',
        'start': 'start_date',
        'position': 'geometry',
        'type_id': 'type_id',
    }

    metadata = MetaData(bind=engine)
    missions = Table('missions', metadata, autoload=True)


    # Insertar los datos
    insert_stmt = missions.insert().values(values_to_insert)
    session.execute(insert_stmt)
    session.commit()
    session.close()






default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'create_fire',
    default_args=default_args,
    description='DAG que envia emails',
    schedule_interval=None,
    catchup=False
)

# Manda correo
print_message_task = PythonOperator(
    task_id='print_message',
    python_callable=print_message,
    provide_context=True,
    dag=dag,
)

atc_create_fire_task = PythonOperator(
    task_id='atc_create_fire',
    python_callable=create_fire,
    provide_context=True,
    dag=dag,
)


print_message_task >> atc_create_fire_task