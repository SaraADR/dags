import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone

def consumer_function(message, **kwargs):
    """Procesa el mensaje desde Kafka y almacena la configuración en XCom evitando duplicados."""
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

        print(f"Verificando ejecución previa para DAG: {target_dag}")

        # Obtener el contexto para verificar ejecuciones previas
        ti = kwargs['ti']
        previous_execution = ti.xcom_pull(task_ids="consume_from_kafka", key=target_dag)

        if previous_execution:
            print(f"El DAG {target_dag} ya fue ejecutado con este mensaje. Ignorando duplicado.")
            return None

        print(f"Guardando en XCom para ejecutar DAG: {target_dag}")

        # Guardar en XCom para evitar que se dispare dos veces
        ti.xcom_push(key=target_dag, value=True)

        return {"target_dag": target_dag, "conf": msg_json}

    except json.JSONDecodeError as e:
        print(f"Error al decodificar JSON: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")

def trigger_dag_run(**kwargs):
    """Obtiene la configuración desde XCom y dispara el DAG correspondiente si no ha sido ejecutado ya."""
    ti = kwargs['ti']
    trigger_data = ti.xcom_pull(task_ids="consume_from_kafka")

    if not trigger_data:
        print("No hay datos de trigger disponibles.")
        return

    target_dag = trigger_data["target_dag"]
    conf = trigger_data["conf"]

    print(f"Ejecutando DAG: {target_dag} con configuración: {conf}")

    trigger = TriggerDagRunOperator(
        task_id=f"trigger_{target_dag}",
        trigger_dag_id=target_dag,
        conf=conf,
        execution_date=datetime.now().replace(tzinfo=timezone.utc)
    )
    trigger.execute(context=kwargs)

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

consume_from_kafka = ConsumeFromTopicOperator(
    kafka_config_id="kafka_connection",
    task_id="consume_from_kafka",
    topics=["rabbit_einforex"],
    apply_function=consumer_function,
    commit_cadence="end_of_batch",  # Asegurar que el mensaje no se duplique
    dag=dag,
)

trigger_dag = PythonOperator(
    task_id="trigger_dag_run",
    python_callable=trigger_dag_run,
    provide_context=True,
    dag=dag,
)

consume_from_kafka >> trigger_dag
