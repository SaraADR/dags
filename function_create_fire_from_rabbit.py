from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from airflow.hooks.base_hook import BaseHook

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_fire_message(**context):
    # Mensaje recibido de la cola
    message = context['dag_run'].conf
    if not message:
        print("No message found in the received data.")
        return

    try:
        # Procesar y decodificar el mensaje
        data = json.loads(message.get('data', '{}'))
        fire_id = data.get('id')
        last_update = data.get('lastUpdate')
        start_date = data.get('start')
        position = data.get('position', {})
        latitude = position.get('y')
        longitude = position.get('x')
        srid = position.get('srid')
        
        if not fire_id or not last_update or not start_date:
            print("Incomplete fire data.")
            return

        # Conexión a la base de datos
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

        # Verificar si el incendio ya está asociado a una misión de extinción
        metadata = MetaData(bind=engine)
        mission = Table('mss_mission', metadata, schema='missions', autoload_with=engine)
        mission_fire = Table('mss_mission_fire', metadata, schema='missions', autoload_with=engine)

        existing_mission_query = session.query(mission).join(
            mission_fire, mission_fire.c.mission_id == mission.c.id
        ).filter(mission_fire.c.fire_id == fire_id, mission.c.type_id == 3).first()

        if existing_mission_query:
            # Actualizar el campo `updatetimestamp` si la fecha es posterior
            stored_update_timestamp = existing_mission_query.updatetimestamp
            if datetime.fromisoformat(last_update[:-1]) > stored_update_timestamp:
                existing_mission_query.updatetimestamp = datetime.fromisoformat(last_update[:-1])
                session.commit()
                print(f"Mission for fire_id {fire_id} updated.")
            else:
                print(f"Received update is not newer for fire_id {fire_id}. No action taken.")
        else:
            # Crear nueva misión de extinción
            customer_id = obtenerCustomerId(session, latitude, longitude)
            initial_status = obtenerInitialStatus(session, 3)
            geometry = {
                "type": "Point",
                "crs": {
                    "type": "name",
                    "properties": {"name": f"urn:ogc:def:crs:EPSG::{srid}"}
                },
                "coordinates": [longitude, latitude]
            }

            new_mission = mission.insert().values(
                name=data.get('name', 'Unnamed'),
                start_date=start_date,
                geometry=str(geometry),
                type_id=3,
                customer_id=customer_id,
                status_id=initial_status,
                updatetimestamp=last_update
            )
            result = session.execute(new_mission)
            mission_id = result.inserted_primary_key[0]

            # Asociar incendio con misión
            mission_fire_insert = mission_fire.insert().values(
                mission_id=mission_id,
                fire_id=fire_id
            )
            session.execute(mission_fire_insert)

            # Registrar estado inicial de la misión
            mission_status_history = Table('mss_mission_status_history', metadata, schema='missions', autoload_with=engine)
            mission_status_history_insert = mission_status_history.insert().values(
                mission_id=mission_id,
                status_id=initial_status,
                updatetimestamp=datetime.now(),
                source="ALGORITHM",
                username="ALGORITHM"
            )
            session.execute(mission_status_history_insert)
            session.commit()
            print(f"New mission created for fire_id {fire_id}.")

    except Exception as e:
        session.rollback()
        print(f"Error processing fire message: {str(e)}")
    finally:
        session.close()

# Función para obtener el estado inicial
def obtenerInitialStatus(session, mission_type):
    try:
        query = f"""
            SELECT status_id 
            FROM missions.mss_mission_initial_status 
            WHERE mission_type_id = {mission_type}
        """
        result = session.execute(query).fetchone()
        return result[0] if result else 1
    except Exception as e:
        print(f"Error fetching initial status: {e}")
        return 1

# Función para obtener el customer_id
def obtenerCustomerId(session, latitude, longitude, epsg=4326):
    try:
        query = f"""
            SELECT customer_id
            FROM missions.mss_extinguish_customers
            WHERE ST_Contains(
                geometry,
                ST_GeomFromText('POINT({longitude} {latitude})', {epsg})
            )
        """
        result = session.execute(query).fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"Error fetching customer_id: {e}")
        return None

dag = DAG(
    'process_fire_mission',
    default_args=default_args,
    description='Process fire messages and create or update missions',
    schedule_interval=None,
    catchup=False
)

process_fire_task = PythonOperator(
    task_id='process_fire_message',
    python_callable=process_fire_message,
    provide_context=True,
    dag=dag,
)

process_fire_task
