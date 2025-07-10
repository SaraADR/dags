import json
import uuid
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
import os
from utils.kafka_headers import extract_trace_id

KAFKA_RAW_MESSAGE_PREFIX = "Mensaje crudo:"

def consumer_function(message, **kwargs):
    """ Procesa el mensaje desde Kafka y dispara el DAG correspondiente. """
    if not message:
        print("Mensaje vacío recibido.")
        return None
    
    print(f"{KAFKA_RAW_MESSAGE_PREFIX} {message}")
    
    trace_id, log_msg = extract_trace_id(message)
    print(log_msg)

    msg_value = message.value().decode('utf-8')
    print(f"Mensaje consumido:\n{msg_value}")

    try:
        msg_json = json.loads(msg_value)
        event_name = msg_json.get("eventName", "")

        if event_name == "GIFAlgorithmExecutionEvent":
            target_dag = "algorithm_gifs_fire_prediction_post_process"
        elif event_name == "FireEvolutionVectorCreatedOrUpdatedEvent":
            target_dag = "algorithm_work_zones"
        else:
            target_dag = "function_create_fire_from_rabbit"

        # Ejecutar el DAG correspondiente
        TriggerDagRunOperator(
            task_id=str(uuid.uuid4()),  # ID único
            trigger_dag_id=target_dag,
            conf=msg_json,  # Pasar el JSON completo
            execution_date=datetime.now().replace(tzinfo=timezone.utc),
            dag=dag
        ).execute(context=kwargs)

    except json.JSONDecodeError as e:
        print(f"Error al decodificar JSON: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")

def there_was_kafka_message(**context):
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    task_id = 'consume_from_topic_minio'
    log_base = "/opt/airflow/logs"
    log_path = f"{log_base}/dag_id={dag_id}/run_id={run_id}/task_id={task_id}"
    
    # Search for the latest log file
    try:
        latest_log = max(
            (os.path.join(root, f) for root, _, files in os.walk(log_path) for f in files),
            key=os.path.getctime
        )
        with open(latest_log, 'r') as f:
            content = f.read()
            return f"{KAFKA_RAW_MESSAGE_PREFIX} <cimpl.Message object at" in content
    except (ValueError, FileNotFoundError):
        return False

# Configuración del DAG
default_args = {
    'owner': 'oscar',
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