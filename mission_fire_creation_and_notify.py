from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import pytz
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text


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

        # Iniciando una transacción
        with session.begin():
            # Obtener el estado inicial desde la tabla tipo_misión - status_inicial
            result = session.execute(f"SELECT status_id FROM missions.mss_mission_initial_status WHERE mission_type_id = {input_data['type_id']}")
            row = result.fetchone()
            if row:
                initial_status = row.status_id
            else:
                initial_status = 1


            # Valores para insertar en la tabla mss_mission
            values_to_insert = {
                'name': input_data['fire']['name'],
                'start_date': input_data['fire']['start'],
                'geometry': '{ "type": "Point", "crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4326" } }, "coordinates": [ '
                            + input_data['fire']['position']['x'] + ', ' + input_data['fire']['position']['y'] + ' ] }',
                'type_id': input_data['type_id'],
                'status_id': initial_status,  # Asignación dinámica del estado inicial
                'customer_id': input_data['customer_id'],
                'alias': input_data['alias']
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

        if input_data['type_id'] == 3:
            fire_id = create_fire(input_data) 
        else:
            fire_id = input_data['fireId']

        if not (fire_id == None or fire_id == 0):
            insert_relation_mission_fire(mission_id, fire_id)

        # Inserción en la tabla mss_mission_status_history
        mission_status_history = Table('mss_mission_status_history', metadata, schema='missions', autoload_with=engine)
        status_history_values = {
            'mission_id': mission_id,
            'status_id': initial_status,  # Ahora toma el status inicial que corresponda
        }
        insert_status_stmt = mission_status_history.insert().values(status_history_values)
        session.execute(insert_status_stmt)
        session.commit()
        print(f"Estado de la misión {mission_id} registrado en mss_mission_status_history.")

        # Almacenar mission_id en XCom para ser utilizado por otras tareas
        input_data['mission_id'] = mission_id
        context['task_instance'].xcom_push(key='mission_id', value=mission_id)

        # Update job status to 'FINISHED'
        jobs = Table('jobs', metadata, schema='public', autoload_with=engine)
        update_stmt = jobs.update().where(jobs.c.id == job_id).values(status='FINISHED')
        session.execute(update_stmt)
        session.commit()
        print(f"Job ID {job_id} status updated to FINISHED")

        if input_data.get('loadMission', False) is True:
            user = message['message']['from_user']
            insert_notification(mission_id, user)

    except Exception as e:
        session.rollback()
        print(f"Error durante el guardado de la misión: {str(e)}")
        jobs = Table('jobs', metadata, schema='public', autoload_with=engine)
        
        # Actualizar el estado del trabajo a "ERROR"
        update_stmt = jobs.update().where(jobs.c.id == job_id).values(status='ERROR')
        session.execute(update_stmt)
        session.commit()
        print(f"Job ID {job_id} status updated to ERROR")

        # Lanzar la excepción para que la tarea falle
        raise RuntimeError(f"Error durante el guardado de la misión: {str(e)}")


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
            print("id del fire: ")
            print(fire_data['id'])
            return fire_data['id']
            
        else:
            print(f"Error en la creación del incendio: {response.status_code}")
            print(response.text)

    except Exception as e:
        print(f"Error al crear el incendio: {str(e)}")


# Función para insertar una relación entre misión e incendio en la base de datos
def insert_relation_mission_fire(id_mission, id_fire):
    try:
        #Conexión a la base de datos usando las credenciales almacenadas en Airflow
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

        values_to_insert = {
            'mission_id': id_mission,
            'fire_id': id_fire,
            'ignition_timestamp': None,
            'stabilization_timestamp': None,
            'controlled_timestamp': None,
            'extinguishing_timestamp': None,
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
        # session.rollback()
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


def insert_notification(id_mission, user):

    if id_mission is not None:
        #Añadimos notificacion
        try:
            db_conn = BaseHook.get_connection('biobd')
            connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
            engine = create_engine(connection_string)
            Session = sessionmaker(bind=engine)
            session = Session()

            data_json = json.dumps({
                "to": str(user),
                "actions":[
                    {
                    "type":"loadMission",
                        "data":{
                        "missionId":id_mission
                        }
                    },
                    {
                    "type": "notify",
                    "data": {
                    "message": f"Misión de incendios creada con id {id_mission}"
                    }
                }]
            }, ensure_ascii=False)

            time = datetime.now().replace(tzinfo=timezone.utc)

            query = text("""
                INSERT INTO public.notifications
                (destination, "data", "date", status)
                VALUES (:destination, :data, :date, NULL);
            """)
            session.execute(query, {
                'destination': 'ignis',
                'data': data_json,
                'date': time
            })
            session.commit()

        except Exception as e:
            session.rollback()
            print(f"Error durante la inserción de la notificación: {str(e)}")
        finally:
            session.close()


# Configuración por defecto para el DAG
default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Definición del DAG
dag = DAG(
    'mission_fire_creation_and_notify',
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

trigger_conversion_task = TriggerDagRunOperator(
    task_id='trigger_convert_ts_to_mp4',
    trigger_dag_id='convert_ts_to_mp4_dag',  
    conf={'video_key': 'uuid12345/video_prueba.ts'}, 
    dag=dag, 
)



# Modifica la secuencia de tareas
print_message_task >> create_mission_task >> trigger_conversion_task