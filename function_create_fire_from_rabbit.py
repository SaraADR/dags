from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine, Table, MetaData, text
from sqlalchemy.orm import sessionmaker
from dag_utils import update_job_status, throw_job_error, get_db_session


def receive_data_and_create_fire(**context):
    message = context['dag_run'].conf
    if not message:
        print("No 'message' field found in the received data.")
        return

    try:
        # Extract 'data' field from the message
        data_str = message.get('data')
        if not data_str:
            print("No 'data' field found in the 'message'.")
            return

        # Decode 'data' field (assuming it is mostly JSON)
        data = json.loads(data_str) if isinstance(data_str, str) else data_str
        print(f"Received message data: {data}")
        
        # Call function to create mission, fire, and history
        mission_id = createMissionMissionFireAndHistoryStatus(data)
        
        # Push mission_id to XCom for downstream tasks
        context['task_instance'].xcom_push(key='mission_id', value=mission_id)

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as e:
        print(f"Unhandled error: {e}")
        raise


def createMissionMissionFireAndHistoryStatus(msg_json):
    try:
        fire_id = msg_json.get('id')
        position = msg_json.get('position', {})
        latitude = position.get('y', None)
        longitude = position.get('x', None)
        srid = position.get('srid', None)

        session = get_db_session()
        engine = session.get_bind()

        # Check if an existing mission needs updating
        existing_mission = session.execute(f"""
            SELECT mission_id, updatetimestamp
            FROM missions.mss_mission_fire mf
            JOIN missions.mss_mission m ON mf.mission_id = m.id
            WHERE mf.fire_id = {fire_id} AND m.type_id = 3
        """).fetchone()

        if existing_mission:
            existing_updatetimestamp = existing_mission['updatetimestamp']
            new_updatetimestamp = datetime.fromisoformat(msg_json.get('lastUpdate'))

            if new_updatetimestamp > existing_updatetimestamp:
                session.execute(f"""
                    UPDATE missions.mss_mission
                    SET updatetimestamp = '{new_updatetimestamp}'
                    WHERE id = {existing_mission['mission_id']}
                """)
                session.commit()
                print(f"Updated mission {existing_mission['mission_id']} with new updatetimestamp.")
            else:
                print("No update required; received timestamp is not newer.")
            return existing_mission['mission_id']

        # Get customer ID and initial status
        customer_id = obtenerCustomerId(session, latitude, longitude)
        initial_status = obtenerInitialStatus(session, 3)

        # Prepare geometry
        geometry = f"{{'type': 'Point', 'crs': {{'type':'name','properties': {{'name': 'urn:ogc:def:crs:EPSG::{srid}' }} }},'coordinates': [{longitude},{latitude}]}}"

        # Insert new mission
        mss_mission_insert = {
            'name': msg_json.get('name', 'noname'),
            'start_date': msg_json.get('start'),
            'geometry': geometry,
            'type_id': 3,
            'customer_id': customer_id,
            'status_id': initial_status,
            'updatetimestamp': msg_json.get('lastUpdate')
        }

        metadata = MetaData(bind=engine)
        mission_table = Table('mss_mission', metadata, schema='missions', autoload_with=engine)
        result = session.execute(mission_table.insert().values(mss_mission_insert))
        session.commit()

        mission_id = result.inserted_primary_key[0]
        print(f"Mission created with ID: {mission_id}")

        # Create mission-fire relation
        mission_fire_table = Table('mss_mission_fire', metadata, schema='missions', autoload_with=engine)
        session.execute(mission_fire_table.insert().values({'mission_id': mission_id, 'fire_id': fire_id}))
        session.commit()

        # Insert mission status history
        mission_status_history_table = Table('mss_mission_status_history', metadata, schema='missions', autoload_with=engine)
        session.execute(mission_status_history_table.insert().values({
            'mission_id': mission_id,
            'status_id': initial_status,
            'updatetimestamp': datetime.now(),
            'source': 'ALGORITHM',
            'username': 'ALGORITHM'
        }))
        session.commit()

        return mission_id

    except Exception as e:
        session.rollback()
        print(f"Error: {e}")
        raise


def obtenerInitialStatus(session, missionType=3):
    try:
        query = f"SELECT status_id FROM missions.mss_mission_initial_status WHERE mission_type_id = {missionType}"
        result = session.execute(query).fetchone()
        return result[0] if result else 1
    except Exception as e:
        print(f"Error fetching initial status: {e}")
        return 1


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
        return result['customer_id'] if result else None
    except Exception as e:
        print(f"Error fetching customer ID: {e}")
        return None


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
    description='DAG that creates fire missions from RabbitMQ events',
    schedule_interval=None,
    catchup=False
)

receive_data_process = PythonOperator(
    task_id='receive_and_create_fire',
    python_callable=receive_data_and_create_fire,
    provide_context=True,
    dag=dag,
)

receive_data_process
