import json
import uuid
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # Import corregido
from datetime import datetime, timedelta, timezone

def consumer_function(message, **kwargs):
    """ Procesa el mensaje desde Kafka y devuelve la configuración para disparar el DAG. """
    if not message:
        print("Mensaje vacío recibido.")
        return None

    msg_value = message.value().decode('utf-8')
    print(f"Mensaje consumido:\n{msg_value}")

    try:
        msg_json = json.loads(msg_value)
        event_name = msg_json.get("eventName", "")

        # Determinar el DAG objetivo
        target_dag = (
            "algorithm_gifs_fire_prediction_post_process"
            if event_name == "GIFAlgorithmExecution"
            else "function_create_fire_from_rabbit"
        )

        print(f"Se procesará el mensaje para disparar el DAG: {target_dag}")

        # Devolver los datos que necesita la siguiente tarea
        return {"dag_id": target_dag, "conf": msg_json}

    except json.JSONDecodeError as e:
        print(f"Error al decodificar JSON: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")

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

def trigger_dag_function(**kwargs):
    """ Extrae los datos del XCom y dispara el DAG adecuado """
    ti = kwargs['ti']
    message_data = ti.xcom_pull(task_ids='consume_from_topic')

    if message_data:
        dag_id = message_data["dag_id"]
        conf = message_data["conf"]

        print(f"Disparando el DAG {dag_id} con configuración: {conf}")

        TriggerDagRunOperator(
            task_id="trigger_dag_run",
            trigger_dag_id=dag_id,
            conf=conf,
            execution_date=datetime.now().replace(tzinfo=timezone.utc),
            dag=dag
        ).execute(context=kwargs)

trigger_dag = PythonOperator(
    task_id="trigger_dag",
    python_callable=trigger_dag_function,
    provide_context=True,
    dag=dag
)

consume_from_topic >> trigger_dag  # Definir dependencias correctamente
