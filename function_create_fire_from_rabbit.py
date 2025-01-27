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
        # Extraer el tipo de evento y los datos
        event_name = message.get('eventName')
        data_str = message.get('data')

        if not event_name or not data_str:
            print("No 'eventName' or 'data' field found in the message.")
            return

        data = json.loads(data_str) if isinstance(data_str, str) else data_str
        print(f"Received event: {event_name}, data: {data}")

        # Manejar cada evento de forma específica
        if event_name == 'FirePerimeterCreatedOrUpdatedEvent':
            handle_fire_perimeter_event(data)
        elif event_name == 'WaterDischargeCreatedOrUpdatedEvent':
            handle_water_discharge_event(data)
        elif event_name == 'FireEvolutionVectorCreatedOrUpdatedEvent':
            handle_fire_evolution_vector_event(data)
        elif event_name == 'CarouselCreatedOrUpdatedEvent':
            handle_carousel_event(data)
        elif event_name == 'FirePerimeterRiskCreatedOrUpdatedEvent':
            handle_fire_perimeter_risk_event(data)
        else:
            print(f"Unhandled event type: {event_name}")

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as e:
        print(f"Unhandled error: {e}")
        raise


# Función para manejar eventos de FirePerimeter
def handle_fire_perimeter_event(data):
    print("Handling FirePerimeterCreatedOrUpdatedEvent")
    # Implementar lógica para manejar el evento
    # Ejemplo: insertar datos en una tabla específica


# Función para manejar eventos de WaterDischarge
def handle_water_discharge_event(data):
    print("Handling WaterDischargeCreatedOrUpdatedEvent")
    # Implementar lógica para manejar el evento
    # Ejemplo: actualizar datos en otra tabla


# Función para manejar eventos de FireEvolutionVector
def handle_fire_evolution_vector_event(data):
    print("Handling FireEvolutionVectorCreatedOrUpdatedEvent")
    # Implementar lógica para manejar el evento
    # Ejemplo: procesar vectores y guardar en la base de datos


# Función para manejar eventos de Carousel
def handle_carousel_event(data):
    print("Handling CarouselCreatedOrUpdatedEvent")
    # Implementar lógica para manejar el evento
    # Ejemplo: registrar cambios en elementos multimedia


# Función para manejar eventos de FirePerimeterRisk
def handle_fire_perimeter_risk_event(data):
    print("Handling FirePerimeterRiskCreatedOrUpdatedEvent")
    # Implementar lógica para manejar el evento
    # Ejemplo: asociar riesgos con el perímetro del incendio


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
    description='DAG que procesa diferentes eventos relacionados con incendios desde RabbitMQ',
    schedule_interval=None,
    catchup=False
)

receive_data_process = PythonOperator(
    task_id='receive_and_process_event',
    python_callable=receive_data_and_create_fire,
    provide_context=True,
    dag=dag,
)

receive_data_process
