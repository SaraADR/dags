import json
from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.python import PythonOperator
from airflow.api.common.experimental.trigger_dag import trigger_dag
from datetime import datetime, timedelta

def handle_message(message, **kwargs):
    try:
        msg_dict = json.loads(message)
    except json.JSONDecodeError:
        return None

    if msg_dict.get('destination') == 'email':
        trigger_dag(dag_id='sendmessage3', run_id=None, conf=msg_dict)
        return msg_dict
    return None

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kafka_listener_dag',
    default_args=default_args,
    description='DAG that listens to Kafka and triggers other DAGs',
    schedule_interval='*/5 * * * *',
    catchup=False,
)

consume_task = ConsumeFromTopicOperator(
    task_id='consume_from_kafka',
    kafka_config_id="kafka_connection",
    topics=['test1'],
    apply_function='kafka_listener_dag.handle_message',
    dag=dag,
)

consume_task
