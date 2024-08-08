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
            # 'geometry': '{ "type": "Point", "crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4326" } }, "coordinates": [ '+input_data['fire']['position']['x']+', '+input_data['fire']['position']['y']+' ] }',
            'geometry': 11,
            'type_id': input_data['type_id'],
            'status_id': 1, #TODO REVISIÓN DE STATUS
            'customer_id': input_data ['customer_id'],
        }

        # Metadatos y tabla de misión en la base de datos
        metadata = MetaData(bind=engine)
        missions = Table('mss_mission', metadata, schema='missions', autoload_with=engine)

        # Inserción de la nueva misión
        insert_stmt = missions.insert().values(values_to_insert)
        result = session.execute(insert_stmt)
        mission_id = result.inserted_primary_key[0]
        session.commit()
        session.close()

        print(f"Misión creada con ID: {mission_id}")

        #  Almacenar mission_id en XCom para ser utilizado por otras tareas
        input_data['mission_id'] = mission_id
        context['task_instance'].xcom_push(key='mission_id', value=mission_id)

        #TODO insertar status,mission_id en mission_status_history
        #Igual hay que insertar el usuario
        # Inserción en la tabla mission_status_history


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

        if job_id.get('job') == 'RETRY':
                trigger = TriggerDagRunOperator(
                task_id='retry_create_fire',
                trigger_dag_id='algorithm_automaps',
                conf={'message': job_id}, 
                execution_date=datetime.now().replace(tzinfo=timezone.utc),
                dag=dag,
            )

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
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

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

# Función para enviar una notificación y almacenarla en la base de datos
def send_notification(mission_id, message, status="enviada"):
    db_conn = BaseHook.get_connection('biobd')
    connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        values_to_insert = {
            'mission_id': mission_id,
            'message': json.dumps(message),
            'status': status,
            'created_at': datetime.now()
        }

        # Metadatos y tabla de notificaciones en la base de datos
        metadata = MetaData(bind=engine)
        notifications = Table('notifications', metadata, schema='missions', autoload_with=engine)

        # Inserción de la notificación
        insert_stmt = notifications.insert().values(values_to_insert)
        session.execute(insert_stmt)
        session.commit()
        session.close()
        print("Notificación enviada y guardada en la base de datos.")
    except Exception as e:
        session.rollback()
        print(f"Error durante el envío de la notificación: {e}")

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
print_message_task >> create_mission_task  >> process_notification_task
