from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, DateTime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy.orm import sessionmaker

# Función para imprimir un mensaje desde la configuración del DAG
def print_message(**context):
    message = context['dag_run'].conf
    print(f"Received message: {message}")

# Función para crear una misión en la base de datos
def create_mission(**context):
    message = context['dag_run'].conf
    print(f"Received message: {message}")
    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str)
    print(input_data)
    job_id = message['message']['id']  # Extracting job_id from the message

    try:
        # Conexión a la base de datos usando las credenciales almacenadas en Airflow
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

        # Transformación de la posición GeoJSON a WKT
        geojson_data = input_data['fire']['position']
        # geometry = geojson_to_wkt(geojson_data)
        values_to_insert = {
            'name': input_data['fire']['name'],
            'start_date': input_data['fire']['start'],
            'geometry': '{ "type": "Point", "crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4326" } }, "coordinates": [ '+input_data['fire']['position']['x']+', '+input_data['fire']['position']['y']+' ] }',
            'type_id': input_data['type_id'],
            'status_id': 1, #TODO REVISIÓN DE STATUS
            'customer_id': input_data ['customer_id'],
            'execution_date': datetime.now() 

        }

        # Inserción de la misión
        metadata = MetaData()
        missions = Table('missions', metadata, autoload_with=engine)
        insert_stmt = missions.insert().values(values_to_insert)
        result = session.execute(insert_stmt)
        mission_id = result.inserted_primary_key[0]

        # Inserción en la tabla mss_mission_status_history
        mission_status_history = Table('mss_mission_status_history', metadata, autoload_with=engine)
        status_history_insert = mission_status_history.insert().values({
            'mission_id': mission_id,
            'status': values_to_insert['status_id'],
            'timestamp': datetime.now()
        })
        session.execute(status_history_insert)
        session.commit()

        session.close()
        print(f"Misión creada con ID {mission_id} y guardada en la base de datos.")
    except Exception as e:
        session.rollback()
        print(f"Error durante la creación de la misión: {e}")

# Función para procesar una notificación después de la creación de una misión
def process_notification(**context):
    # Recuperar mission_id del contexto
    mission_id = context['task_instance'].xcom_pull(key='mission_id')
    message = {"text": "Nueva misión creada", "details": "Detalles adicionales"}
    # Enviar la notificación
    send_notification(mission_id, message)

# Configuración por defecto para el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Definición del DAG
dag = DAG(
    'create_fire',
    default_args=default_args,
    description='DAG que maneja la creación de misiones e incendios',
    schedule_interval=None,
    catchup=False
)

# Definición de las tareas del DAG
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

process_notification_task = PythonOperator(
    task_id='process_notification',
    python_callable=process_notification,
    provide_context=True,
    dag=dag,
)

# Definición de la secuencia de tareas en el DAG
print_message_task >> create_mission_task >> process_notification_task
