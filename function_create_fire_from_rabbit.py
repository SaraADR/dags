from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from jinja2 import Template
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker



def receive_data_and_create_fire(**context):
    message = context['dag_run'].conf
    if not message:
        print("No 'message' field found in the received data.")
        return

    try:
        # Extraemos el campo 'data' que está dentro del 'message'
        data_str = message.get('data')
        if not data_str:
            print("No 'data' field found in the 'message'.")
            return

        # Si el campo 'data' es una cadena, lo decodificamos como JSON (99% veces va a ser)
        data = json.loads(data_str) if isinstance(data_str, str) else data_str
        print(f"Received message data: {data}")
        # aqui es donde se inserta en bd
        # INSERT EN BD CREATE FIRE
        return createMissionMissionFireAndHistoryStatus(data)

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return

    return 



def createMissionMissionFireAndHistoryStatus(msg_json):
    try:
        fire_id = msg_json.get('id')    
        #fire_name = msg_json.get('name', 'noname')
        position = msg_json.get('position', {})
        latitude = position.get('y', None)
        longitude = position.get('x', None)
        srid = position.get('srid', None)
        
        try:
            #Insertamos la mision
            db_conn = BaseHook.get_connection('biobd')
            connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
            engine = create_engine(connection_string)
            Session = sessionmaker(bind=engine)
            session = Session()

            # Verificar si ya existe una misión de Extinción asociada al incendio
            existing_mission = session.execute(f"""
                SELECT mission_id, updatetimestamp
                FROM missions.mss_mission_fire mf
                JOIN missions.mss_mission m ON mf.mission_id = m.id
                WHERE mf.fire_id = {fire_id} AND m.type_id = 3
            """).fetchone()

            if existing_mission:
                # Comparar fechas de actualización
                existing_updatetimestamp = existing_mission['updatetimestamp']
                new_updatetimestamp = datetime.fromisoformat(msg_json.get('lastUpdate'))

                if new_updatetimestamp > existing_updatetimestamp:
                    # Actualizar el updatetimestamp en la misión existente
                    session.execute(f"""
                        UPDATE missions.mss_mission
                        SET updatetimestamp = '{new_updatetimestamp}'
                        WHERE id = {existing_mission['mission_id']}
                    """)
                    session.commit()
                    print(f"Updated mission {existing_mission['mission_id']} with new updatetimestamp.")
                else:
                    print("No update required; received timestamp is not newer.")
                return  # No se crea una nueva misión


            # Query para extraer el customer_id
            customer_id = obtenerCustomerId(session, latitude, longitude)
            print(customer_id)

            # Obtenemos initial status
            initial_status = obtenerInitialStatus(session, 3)
            print(initial_status)

            # Componemos geometría
            geometry = f"{{'type': 'Point', 'crs': {{'type':'name','properties': {{'name': 'urn:ogc:def:crs:EPSG::{srid}' }} }},'coordinates': [{longitude},{latitude}]}}"
            print(geometry)

            mss_mission_insert = {
                #id es auto_increment; alias es NULL; service_id es NULL, end_date es NULL
                'name': msg_json.get('name', 'noname'),
                'start_date': msg_json.get('start'),
                'geometry': geometry,
                'type_id': 3,
                'customer_id': customer_id,
                #'creationtimestamp': creation_date, # AHORA es now() porque es creación de la MISIÓN
                'status_id': initial_status,
                'updatetimestamp': msg_json.get('lastUpdate')
            }
            

            metadata = MetaData(bind=engine)
            mission = Table('mss_mission', metadata, schema='missions', autoload_with=engine)

            # Inserción 
            insert_stmt = mission.insert().values(mss_mission_insert)
            #Guardamos el resultado para traer el id
            result = session.execute(insert_stmt)
            session.commit()
            session.close()

            mission_id = result.inserted_primary_key[0]
            print(f"Misión creada con ID: {mission_id}")
        except Exception as e:
            session.rollback()
            print(f"Error durante el guardado de la misión: {str(e)}")
            raise Exception("Error durante el guardado del estado de la misión")

        try:
            if (mission_id is not None):
                #Insertamos la mision_fire
                db_conn = BaseHook.get_connection('biobd')
                connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
                engine = create_engine(connection_string)
                Session = sessionmaker(bind=engine)
                session = Session()

                mss_mission_fire_insert = {
                    'mission_id': mission_id,
                    # 'ignition_timestamp': ignition_date,
                    'fire_id': fire_id
                }
            

                metadata = MetaData(bind=engine)
                mission_fire = Table('mss_mission_fire', metadata, schema='missions', autoload_with=engine)

                # Inserción de la relación
                insert_stmt = mission_fire.insert().values(mss_mission_fire_insert)
                session.execute(insert_stmt)
                session.commit()
                session.close()
        except Exception as e:
            session.rollback()
            print(f"Error durante el guardado de la relacion mission fire: {str(e)}")
            raise Exception("Error durante el guardado de la relacion mission fire")

        try:
            if (mission_id is not None):
                #Insertamos la mision_fire
                db_conn = BaseHook.get_connection('biobd')
                connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
                engine = create_engine(connection_string)
                Session = sessionmaker(bind=engine)
                session = Session()

                mss_mission_history_state_insert = {
                    'mission_id': mission_id,
                    'status_id': initial_status,
                    'updatetimestamp': datetime.now(),
                    'source': 'ALGORITHM',
                    'username': 'ALGORITHM'
                }
            

                metadata = MetaData(bind=engine)
                mission_status_history = Table('mss_mission_status_history', metadata, schema='missions', autoload_with=engine)

                # Inserción de la relación
                insert_stmt = mission_status_history.insert().values(mss_mission_history_state_insert)
                session.execute(insert_stmt)
                session.commit()
                session.close()
        except Exception as e:
            session.rollback()
            print(f"Error durante el guardado del estado de la misión: {str(e)}")   
            raise Exception("Error durante el guardado del estado de la misión")

 
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        # Variable.delete("mensaje_save")


# TODO: PASAR A UTILS estas funciones?
def obtenerInitialStatus(session, missionType=3):
    try:
        query = f"""
            SELECT status_id 
            FROM missions.mss_mission_initial_status 
            WHERE mission_type_id = {missionType}
        """
        result = session.execute(query)
        rows = result.fetchall()
        if rows:
            return rows[0][0]  
        else:
            return 1  
    except Exception as e:
        print(f"Error fetching initial status: {e}")
        return 1

def obtenerCustomerId(session, latitude, longitude, epsg = 4326):
    try:
        result = session.execute(f"""
            SELECT customer_id
            FROM missions.mss_extinguish_customers
            WHERE ST_Contains(
                geometry,
                ST_GeomFromText('POINT({longitude} {latitude})',{epsg})
            )"""
        )
        if result.length() > 0:
            return result[0].customer_id
        else:
            return ""
    except Exception as e:
        return ""

# end todo.

default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'function_create_fire_from_rabbit',
    default_args=default_args,
    description='DAG que crea el fire desde el rabbit',
    schedule_interval=None,
    catchup=False
)

# Manda correo
receive_data_process = PythonOperator(
    task_id='receive_and_create_fire',
    python_callable=receive_data_and_create_fire,
    provide_context=True,
    dag=dag,
)

receive_data_process 
#>> other_task_vacia