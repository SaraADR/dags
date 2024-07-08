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
    


def trigger_sendmessage3_dag(message, **context):
    tipo = message[0]
    print(f" message: {message}")
    print(f" tipo: {tipo}")

    TriggerDagRunOperator(
        trigger_dag_id="enviarfichero",
        task_id=f"triggered_downstram_dag_{uuid.uuid4()}",
        wait_for_completion=True,
        conf={
            "tipo": tipo
        },
        poke_interval=20,

    ).execute(context)

    #message = kwargs['ti'].xcom_pull(task_ids='consume_from_kafka', key='message')
   # if tipo:
    #    trigger_dag(dag_id='sendmessage3', run_id=None, conf=message)

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
    schedule_interval='@continuous',
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
)
def listen_to_the_stream():
    listen_for_message = AwaitMessageTriggerFunctionSensor(
        task_id='consume_from_kafka',
        kafka_config_id="kafka_connection",
        topics=['test1'],
        apply_function="listen_to_the_stream.listen_function", 
        poll_interval=5,
        poll_timeout=1,
        apply_function_kwargs={},
        event_triggered_function=trigger_sendmessage3_dag,
)


listen_to_the_stream()
