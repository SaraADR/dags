from airflow import DAG
from airflow.providers.apache.kafka.operators.kafka import KafkaConsumerOperator
from datetime import datetime

default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
}

with DAG(
    'kafka_airflow_integration', 
    default_args=default_args,
    schedule_interval='@daily'
    ) as dag:

    consume_and_analyze_data = KafkaConsumerOperator(
        task_id='consume_and_analyze_data',
        topic='test1',
        bootstrap_servers='kafka_broker:9092',
        group_id='1',
    )
