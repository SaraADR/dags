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

    if input_data['type_id'] == 3:
        # LLAMAR AL ATC
        print("Es de tipo incendios")
        conn = BaseHook.get_connection('atc_services_connection')
        auth = (conn.login, conn.password)
        # URL del servicio
        url = f"{conn.host}/rest/FireService/save"
        response = requests.post(url, json=input_data['fire'], auth=auth)
        if response.status_code == 200:
            print("Solicitud exitosa:")
            print(response.json())

            # Llamar a create_mission con la respuesta del incendio y el job original
            create_mission(response.json(), input_data_str)
        else:
            print(f"Error en la solicitud: {response.status_code}")
            print(response.text)
    else:
        # POR AHORA NADA
        print("No es de tipo incendios")

def create_mission(fire, job):
    print(fire)
    print(job)
    db_conn = BaseHook.get_connection('biobd')
    connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Convertir GeoJSON a WKT
        geojson_data = fire['position']
        geometry = geojson_to_wkt(geojson_data)

        values_to_insert = {
            'name': fire['name'],
            'start_date': fire['start'],
            'geometry': geometry,
            'type_id': 3, 
            'status_id': 1,
            # Usar el ID del incendio como ID de la misión no es una práctica común pero se usa aquí para simplicidad del ejemplo
            'id': fire['id']
        }

        metadata = MetaData(bind=engine)
        missions = Table('mss_mission', metadata, schema='missions', autoload_with=engine)

        # Insertar los datos
        insert_stmt = missions.insert().values(values_to_insert)
        result = session.execute(insert_stmt)
        mission_id = result.inserted_primary_key[0]  # Obtener el ID de la misión insertada
        session.commit()
        session.close()    

        # Insertar relación entre misión e incendio
        insert_relation_mission_fire(mission_id, fire['id'])

    except Exception as e:
        session.rollback()
        print(f"Error durante el guardado de la misión: {str(e)}")

def insert_relation_mission_fire(id_mission, id_fire):
    db_conn = BaseHook.get_connection('biobd')
    connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        values_to_insert = {
            'mission_id': id_mission,
            'fire_id': id_fire
        }

        metadata = MetaData(bind=engine)
        missions_fire = Table('mss_mission_fire', metadata, schema='missions', autoload_with=engine)

        # Insertar los datos
        insert_stmt = missions_fire.insert().values(values_to_insert)
        session.execute(insert_stmt)
        session.commit()
        session.close()

    except Exception as e:
        session.rollback()
        print(f"Error durante el guardado de la relación misión-incendio: {str(e)}")

def geojson_to_wkt(geojson):
    x = geojson['x']
    y = geojson['y']
    z = geojson['z']

    x = float(x) if x is not None else None
    y = float(y) if y is not None else None
    if z is not None:
        z = float(z) if z is not None else None
        return f"POINT ({x} {y} {z})"
    return f"POINT ({x} {y} 0)"

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
    description='DAG que maneja la creación de incendios y misiones',
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