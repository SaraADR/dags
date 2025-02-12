from datetime import datetime, timedelta, timezone
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
        print("No se encontró el campo 'message' en los datos recibidos.")
        return

    try:
        # Extraer el campo 'eventName' y 'data' del mensaje
        event_name = message.get('eventName')
        data_str = message.get('data')
        
        if not event_name:
            print("Advertencia: No se encontró el campo 'eventName' en el mensaje.")
            event_name = "UnknownEvent"  # Asignar un valor predeterminado si falta
        
        if not data_str:
            print("No se encontró el campo 'data' en el mensaje.")
            return
        
        # Decodificar el campo 'data' (suponiendo que es un JSON)
        data = json.loads(data_str) if isinstance(data_str, str) else data_str
        print(f"Datos del mensaje recibidos: {data}")
        print(f"Evento recibido: {event_name}")

        # Determinar la acción en función del tipo de evento
        if event_name == 'FireCreatedOrUpdatedEvent':
            # Mantener el comportamiento original para FireCreatedOrUpdatedEvent
            mission_id = createMissionMissionFireAndHistoryStatus(data)
            notify_frontend(mission_id, message.get('from_user', 'all_users'))
        elif event_name in (
            'FirePerimeterCreatedOrUpdatedEvent',
            'WaterDischargeCreatedOrUpdatedEvent',
            'FireEvolutionVectorCreatedOrUpdatedEvent',
            'CarouselCreatedOrUpdatedEvent',
            'FirePerimeterRiskCreatedOrUpdatedEvent'
        ):
            # Ejecutar la lógica nueva para los otros eventos
            handle_additional_event(data, event_name)
        else:
            print(f"Evento no reconocido: {event_name}")
            return

    except json.JSONDecodeError as e:
        print(f"Error al decodificar el JSON: {e}")
        raise e
    except Exception as e:
        print(f"Error no manejado: {e}")
        raise e


def notify_frontend(mission_id, user):
    """
    Notificar al sistema front-end sobre la creación o actualización de una misión.
    """
    try:
        if mission_id is not None:
            session = get_db_session()

            # Crear la carga útil para la notificación
            data_json = json.dumps({
                "to": str(user),
                "actions": [
                    {
                        "type": "loadMission",
                        "data": {"missionId": mission_id}
                    },
                    {
                        "type": "notify",
                        "data": {"message": f"Misión creada o actualizada con ID: {mission_id}"}
                    }
                ]
            }, ensure_ascii=False)

            # Obtener la hora actual con zona horaria
            time = datetime.now().replace(tzinfo=timezone.utc)

            # Insertar la notificación en la base de datos
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

            print(f"Notificación enviada para la misión con ID: {mission_id}")

    except Exception as e:
        session.rollback()
        print(f"Error al insertar la notificación: {str(e)}")
        raise


def handle_additional_event(data, event_name):
    """
    Lógica para manejar los eventos adicionales como FirePerimeterCreatedOrUpdatedEvent,
    WaterDischargeCreatedOrUpdatedEvent, etc.
    """
    try:
        session = get_db_session()

        # Insertar los nuevos datos en la base de datos o procesarlos según la lógica específica
        print(f"Procesando evento adicional '{event_name}'...")
        fire_id = data.get('fireId')

        # Buscar el mission_id en la base de datos para el fire_id con type_id = 3
        existing_mission = session.execute(f"""
            SELECT m.id
            FROM missions.mss_mission_fire mf
            JOIN missions.mss_mission m ON mf.mission_id = m.id
            WHERE mf.fire_id = {fire_id} AND m.type_id = 3
        """).fetchone()

        if existing_mission:
            mission_id = existing_mission['id']  # Obtener ID de misión si está disponible
            print(f"Procesando el evento {event_name} para la misión ID: {mission_id}")
            # Notificar al front-end sobre los cambios
            notify_frontend_additional_event(mission_id, event_name)
        else:
            print(f"Advertencia: No se encontró una misión en BD para fire_id {fire_id}. "
                  f"Evento {event_name} ignorado.")

    except Exception as e:
        print(f"Error procesando el evento adicional '{event_name}': {e}")



def notify_frontend_additional_event(mission_id, event_name):
    """Notificar al sistema front-end sobre los eventos adicionales."""

    element = ""

    if event_name == 'FirePerimeterCreatedOrUpdatedEvent':
        element = "perimeters"
    elif event_name == 'WaterDischargeCreatedOrUpdatedEvent':
        element = "waterDischarges"
    elif event_name == 'FireEvolutionVectorCreatedOrUpdatedEvent':
        element = "vectors"
    elif event_name == 'CarouselCreatedOrUpdatedEvent':
        element = "norias"
    elif event_name == 'FirePerimeterRiskCreatedOrUpdatedEvent':
        element = "riskElements"
    
    try:
        if mission_id is not None:
            session = get_db_session()

            # Crear la carga útil para la notificación
            data_json = json.dumps({
                "to": "all_users",
                "actions": [
                    {
                        "type": "reloadMissionElements", 
                        "data": {
                            "missionId": mission_id,
                            "elements": [element]
                        
                        }
                    },
                    # TODO: Para entregas Abril: la de texto se quiere?
                    {
                        "type": "notify",
                        "data": {"message": f"Evento {event_name} procesado para la misión ID: {mission_id}"}
                    }
                ]
            }, ensure_ascii=False)

            # Obtener la hora actual con zona horaria
            time = datetime.now().replace(tzinfo=timezone.utc)

            # Insertar la notificación en la base de datos
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

            print(f"Notificación enviada para la misión con ID: {mission_id} y evento {event_name}")

    except Exception as e:
        print(f"Error al insertar la notificación: {str(e)}")
        raise

# Función para crear una misión, una relación misión-incendio y un historial de estado de misión
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

        # Check if a valid result is returned
        if result is not None:
            customer_id = result[0]  # Assuming the query returns a tuple
            if customer_id is not None:
                return customer_id
        
        # Default value if customer_id not is found
        print("No se encontró un customer_id en la tabla, asignando 'BABCOCK' como valor predeterminado.")
        return 'BABCOCK'
    
    except Exception as e:
        print(f"Error fetching customer ID: {e}")
        return 'BABCOCK'  # Default value in case of an error


default_args = {
    'owner': 'oscar',
    'depends_on_past': True,  # Evita que se solapen ejecuciones anteriores
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Mayor tolerancia a fallos
    'retry_delay': timedelta(minutes=5),  # Aumenta el tiempo entre reintentos
}

dag = DAG(
    'function_create_fire_from_rabbit',
    default_args=default_args,
    description='DAG que maneja eventos de incendios y misiones desde RabbitMQ',
    schedule_interval=None,  # Solo se ejecuta cuando se activa manualmente
    catchup=False,  # No procesa tareas atrasadas
    max_active_runs=2,  # Solo una ejecución activa a la vez
    concurrency=2,  # Solo una tarea ejecutándose simultáneamente
)

receive_data_process = PythonOperator(
    task_id='receive_and_process_event',
    python_callable=receive_data_and_create_fire,
    provide_context=True,
    dag=dag,
)


receive_data_process
