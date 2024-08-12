from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import json
import requests
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker


def print_message(**context):
    message = context['dag_run'].conf
    print(f"Received message: {message}")


def create_mission(**context):
    session = None  # Define la variable session fuera del try
    try:
        message = context['dag_run'].conf
        print(f"Received message: {message}")
        input_data_str = message['message']['input_data']
        input_data = json.loads(input_data_str)
        job_id = message['message']['id']

        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

        geojson_data = input_data['fire']['position']
        values_to_insert = {
            'name': input_data['fire']['name'],
            'start_date': input_data['fire']['start'],
            'geometry': '{ "type": "Point", "crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4326" } }, "coordinates": [ '+input_data['fire']['position']['x']+', '+input_data['fire']['position']['y']+' ] }',
            'type_id': input_data['type_id'],
            'status_id': 1, 
            'customer_id': input_data['customer_id'],
        }

        metadata = MetaData(bind=engine)
        missions = Table('mss_mission', metadata, schema='missions', autoload_with=engine)
        insert_stmt = missions.insert().values(values_to_insert)
        result = session.execute(insert_stmt)
        mission_id = result.inserted_primary_key[0]
        session.commit()

        mission_status_history = Table('mss_mission_status_history', metadata, schema='missions', autoload_with=engine)
        status_history_values = {
            'mission_id': mission_id,
            'status_id': 1,
        }
        insert_status_stmt = mission_status_history.insert().values(status_history_values)
        session.execute(insert_status_stmt)
        session.commit()

        input_data['mission_id'] = mission_id
        context['task_instance'].xcom_push(key='mission_id', value=mission_id)

        create_fire(input_data)

        jobs = Table('jobs', metadata, schema='public', autoload_with=engine)
        update_stmt = jobs.update().where(jobs.c.id == job_id).values(status='FINISHED')
        session.execute(update_stmt)
        session.commit()

    except Exception as e:
        if session:
            session.rollback()
        print(f"Error durante el guardado de la misión: {str(e)}")
        raise  # Rethrow the exception to trigger retry

    finally:
        if session:
            session.close()


def create_fire(input_data):
    try:
        conn = BaseHook.get_connection('atc_services_connection')
        auth = (conn.login, conn.password)
        url = f"{conn.host}/rest/FireService/save"
        response = requests.post(url, json=input_data['fire'], auth=auth)

        if response.status_code == 200:
            fire_data = response.json()
            insert_relation_mission_fire(input_data['mission_id'], fire_data['id'])
        else:
            raise Exception(f"Error en la creación del incendio: {response.status_code}, {response.text}")

    except Exception as e:
        print(f"Error al crear el incendio: {str(e)}")
        raise  # Rethrow the exception to trigger retry


def insert_relation_mission_fire(id_mission, id_fire):
    session = None
    try:
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

        values_to_insert = {
            'mission_id': id_mission,
            'fire_id': id_fire
        }

        metadata = MetaData(bind=engine)
        missions_fire = Table('mss_mission_fire', metadata, schema='missions', autoload_with=engine)
        insert_stmt = missions_fire.insert().values(values_to_insert)
        session.execute(insert_stmt)
        session.commit()

    except Exception as e:
        if session:
            session.rollback()
        print(f"Error durante la relación misión-incendio: {str(e)}")
        raise  # Rethrow the exception to trigger retry

    finally:
        if session:
            session.close()


# Definición del DAG y sus tareas
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
    description='DAG que maneja la creación de misiones e incendios',
    schedule_interval=None,
    catchup=False
)

print_message_task = PythonOperator(
    task_id='print_message',
    python_callable=print_message,
    provide_context=True,
    dag=dag,
)

create_mission_task = PythonOperator(
    task_id='create_mission',
    python_callable=create_mission,
    provide_context=True,
    dag=dag,
)

trigger_dag = TriggerDagRunOperator(
    task_id='trigger_another_dag',
    trigger_dag_id='save_documents_to_minio',  # Reemplaza con el ID de tu DAG objetivo
    conf={'message': 'Retry triggered'},
    dag=dag,
)

# Si create_mission falla, se activará el trigger del otro DAG
create_mission_task >> trigger_dag

# Secuencia de tareas en el DAG
print_message_task >> create_mission_task
