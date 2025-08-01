from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, Table, MetaData
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy.orm import sessionmaker
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
from dag_utils import update_job_status, throw_job_error,get_db_session


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
        session = get_db_session()
        engine = session.get_bind()

        result = session.execute(f"SELECT status_id FROM missions.mss_mission_initial_status WHERE mission_type_id = {input_data['type_id']}")
        if result.length() > 0:
            initial_status = result[0].status_id
        else:
            initial_status = 1

        # Transformación de la posición GeoJSON a WKT
        geojson_data = input_data['fire']['position']
        # geometry = geojson_to_wkt(geojson_data)
        values_to_insert = {
            'name': input_data['fire']['name'],
            'start_date': input_data['fire']['start'],
            'geometry': '{ "type": "Point", "crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4326" } }, "coordinates": [ '+input_data['fire']['position']['x']+', '+input_data['fire']['position']['y']+' ] }',
            'type_id': input_data['type_id'],
            'status_id': initial_status, #Tomamos status inicial
            'customer_id': input_data['customer_id'],
        }

        # Metadatos y tabla de misión en la base de datos
        metadata = MetaData(bind=engine)
        missions = Table('mss_mission', metadata, schema='missions', autoload_with=engine)

        # Inserción de la nueva misión
        insert_stmt = missions.insert().values(values_to_insert)
        result = session.execute(insert_stmt)
        mission_id = result.inserted_primary_key[0]
        session.commit()
        print(f"Misión creada con ID: {mission_id}")

        # Inserción en la tabla mss_mission_status_history
        mission_status_history = Table('mss_mission_status_history', metadata, schema='missions', autoload_with=engine)
        status_history_values = {
            'mission_id': mission_id,
            'status_id': initial_status,  # Tomamos el status_inicial
        }
        insert_status_stmt = mission_status_history.insert().values(status_history_values)
        session.execute(insert_status_stmt)
        session.commit()
        print(f"Estado de la misión {mission_id} registrado en mss_mission_status_history.")

        # Almacenar mission_id en XCom para ser utilizado por otras tareas
        input_data['mission_id'] = mission_id
        context['task_instance'].xcom_push(key='mission_id', value=mission_id)

        # Crear el incendio relacionado
        create_fire(input_data)

        # Update job status to 'FINISHED'
        jobs = Table('jobs', metadata, schema='public', autoload_with=engine)
        update_stmt = jobs.update().where(jobs.c.id == job_id).values(status='FINISHED')
        session.execute(update_stmt)
        session.commit()
        print(f"Job ID {job_id} status updated to FINISHED")

    except Exception as e:
        session.rollback()
        print(f"Error durante el guardado de la misión: {str(e)}")
        jobs = Table('jobs', metadata, schema='public', autoload_with=engine)
        update_stmt = jobs.update().where(jobs.c.id == job_id).values(status='RETRY')
        session.execute(update_stmt)
        session.commit()
        print(f"Job ID {job_id} status updated to RETRY")


# Función para crear un incendio a través del servicio ATC
def create_fire(input_data):
    try:
        print("Creando incendio vía servicio ATC...")
        # Conexión al servicio ATC usando las credenciales almacenadas en Airflow
        conn = BaseHook.get_connection('atc_services_connection')
        auth = (conn.login, conn.password)
        url = f"{conn.host}/rest/FireService/save"
        response = requests.post(url, json=input_data['fire'], auth=auth)

        if response.status_code == 200:
            print("Incendio creado con éxito.")
            fire_data = response.json()
            print(fire_data)

            # Relacionar misión con incendio
            insert_relation_mission_fire(input_data['mission_id'], fire_data['id'])
        else:
            print(f"Error en la creación del incendio: {response.status_code}")
            print(response.text)

    except Exception as e:
        print(f"Error al crear el incendio: {str(e)}")

# Función para insertar una relación entre misión e incendio en la base de datos
def insert_relation_mission_fire(id_mission, id_fire):
    try:
        # Conexión a la base de datos usando las credenciales almacenadas en Airflow
        session = get_db_session()
        engine = session.get_bind()

        values_to_insert = {
            'mission_id': id_mission,
            'fire_id': id_fire
        }

        # Metadatos y tabla de relación misión-incendio en la base de datos
        metadata = MetaData(bind=engine)
        missions_fire = Table('mss_mission_fire', metadata, schema='missions', autoload_with=engine)

        # Inserción de la relación
        insert_stmt = missions_fire.insert().values(values_to_insert)
        session.execute(insert_stmt)
        session.commit()
        session.close()

        print(f"Relación misión-incendio creada: misión {id_mission}, incendio {id_fire}")

    except Exception as e:
        session.rollback()
        print(f"Error durante la relación misión-incendio: {str(e)}")

# Función para convertir coordenadas GeoJSON a WKT
def geojson_to_wkt(geojson):
    x = geojson['x']
    y = geojson['y']
    z = geojson.get('z', 0)  # Proporcionar un valor predeterminado de 0 para z

    x = float(x) if x is not None else None
    y = float(y) if y is not None else None
    z = float(z) if z is not None else 0

    return f"POINT ({x} {y} {z})"

# Configuración por defecto para el DAG
default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Definición del DAG de reintento
retry_dag = DAG(
    'mission_retry_fire_creation_and_notify',
    default_args=default_args,
    description='DAG de reintento para la creación de misiones e incendios',
    schedule_interval=None,
    catchup=False
)

# Definición de las tareas del DAG de reintento
retry_print_message_task = PythonOperator(
    task_id='retry_print_message',
    python_callable=print_message,
    provide_context=True,
    dag=retry_dag,
)

retry_create_mission_task = PythonOperator(
    task_id='retry_create_mission',
    python_callable=create_mission,
    provide_context=True,
    dag=retry_dag,
)

# Definición de la secuencia de tareas en el DAG de reintento
retry_print_message_task >> retry_create_mission_task