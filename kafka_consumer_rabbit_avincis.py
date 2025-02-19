import json
import uuid
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

def consumer_function(message, **kwargs):
    """ Procesa el mensaje recibido desde Kafka y lo envía al DAG correspondiente. """
    if message is not None:
        msg_value = message.value().decode('utf-8')
        print("Mensaje consumido:")
        print(f"{msg_value}")

        if msg_value:
            process_message(msg_value, kwargs)
        else:
            print("Mensaje vacío recibido.")
            return None
    else:
        print("Mensaje vacío recibido.")
        return None

def process_message(message, kwargs):
    """ Filtra y redirige el mensaje al DAG correcto, asegurando que el archivo JSON se pase correctamente. """
    try:
        msg_json = json.loads(message)  # Convertir el mensaje a JSON

        # Verificar si el evento es de tipo "GIFAlgorithmExecution"
        event_name = msg_json.get("eventName", "")

        if event_name == "GIFAlgorithmExecution":
            target_dag = "algorithm_gifs_fire_prediction_post_process"
        else:
            target_dag = "function_create_fire_from_rabbit"

        print(f"Redirigiendo evento a DAG: {target_dag}")

        unique_id = str(uuid.uuid4())  # Generar un ID único para la tarea

        # Guardar JSON recibido en un archivo en la carpeta de Airflow
        input_json_path = f"/home/admin3/grandes-incendios-forestales/input_automatic.json"
        with open(input_json_path, "w", encoding="utf-8") as json_file:
            json.dump(msg_json, json_file, ensure_ascii=False, indent=4)

        print(f"Archivo JSON guardado en {input_json_path}")

        # Disparar el DAG correspondiente con la referencia al archivo JSON
        trigger_dag_run = TriggerDagRunOperator(
            task_id=unique_id,
            trigger_dag_id=target_dag,
            conf={"file_path": input_json_path},
            execution_date=datetime.now().replace(tzinfo=timezone.utc),
            dag=dag
        )

        trigger_dag_run.execute(context=kwargs)

    except json.JSONDecodeError as e:
        print(f"Error decodificando JSON: {e}")
    except Exception as e:
        print(f"Error en la ejecución: {e}")

# Configuración del DAG
default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'kafka_consumer_rabbit_avincis',
    default_args=default_args,
    description='DAG que consume eventos de RabbitMQ/Kafka y los redirige según el tipo de evento.',
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1    
)

consume_from_topic = ConsumeFromTopicOperator(
    kafka_config_id="kafka_connection",
    task_id="consume_from_topic",
    topics=["rabbit_einforex"],
    apply_function=consumer_function,
    commit_cadence="end_of_batch",
    dag=dag,
)

consume_from_topic
