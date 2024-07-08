import json
import uuid
from airflow import DAG
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageTriggerFunctionSensor
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import dag

def listen_function(message):
    msg_dict = json.loads(message.value())
    print(f"Full message: {msg_dict}")
    if msg_dict.get('destination') == 'email':
        return msg_dict
    elif msg_dict.get('destination') == 'telegram':
        return msg_dict
    return None

def trigger_sendmessage3_dag(message, **context):
    if not message:
        print("No valid message received.")
        return
    
    tipo = message.get('destination')
    print(f" message: {message}")
    print(f" tipo: {tipo}")

    TriggerDagRunOperator(
        task_id=f"triggered_downstream_dag_{uuid.uuid4()}",
        trigger_dag_id="sendmessage3" if tipo == "email" else "send_telegram_message",
        wait_for_completion=True,
        conf={"tipo": tipo},
        poke_interval=20,
    ).execute(context)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    'kafka_listener_dag',
    default_args=default_args,
    description='DAG that listens to Kafka and triggers other DAGs',
    schedule_interval=None,  # Change '@continuous' to None as '@continuous' is not a valid cron expression
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
)
def listen_to_the_stream():
    listen_for_message = AwaitMessageTriggerFunctionSensor(
        task_id='consume_from_kafka',
        kafka_config_id="kafka_connection",
        topics=['test1'],
        apply_function=f"{__name__}.listen_function",  # Correct reference to the function
        poll_interval=5,
        poll_timeout=1,
        apply_function_kwargs={},
        event_triggered_function=trigger_sendmessage3_dag,
    )

    listen_for_message

listen_to_the_stream()
