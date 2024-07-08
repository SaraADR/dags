from datetime import timedelta
import json
import requests
from pendulum import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.kafka.operators.consume import KafkaConsumerOperator

def get_a_cat_fact(ti):
    """
    Gets a cat fact from the CatFacts API
    """
    url = "http://catfact.ninja/fact"
    res = requests.get(url)
    ti.xcom_push(key="cat_fact", value=json.loads(res.text)["fact"])

def analyze_cat_facts(ti):
    """
    Prints the cat fact
    """
    cat_fact = ti.xcom_pull(key="cat_fact", task_ids="get_a_cat_fact")
    print("Cat fact for today:", cat_fact)

    message_resp = ti.xcom_pull(key="message_resp", task_ids="consume_from_kafka")
    print("Messages received from Kafka:", message_resp)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    "xcomdag",
    description="A DAG to demonstrate Kafka consumption with Apache Kafka",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    get_cat_data = PythonOperator(
        task_id="get_a_cat_fact",
        python_callable=get_a_cat_fact,
    )

    consume_kafka_data = KafkaConsumerOperator(
        task_id="consume_from_kafka",
        topics=["topic1"],  # Reemplaza 'your_topic' con el nombre de tu topic
        kafka_config={
            "bootstrap.servers": "10.96.117.39:9092",  # Reemplaza con la dirección de tu servidor Kafka
            "group.id": "1",  # Reemplaza 'my-group' con el ID de tu grupo de consumidores
            "auto.offset.reset": "earliest",
        },
        max_messages=10,
        xcom_push_key="message_resp",
    )

    analyze_cat_data = PythonOperator(
        task_id="analyze_data",
        python_callable=analyze_cat_facts,
    )

    get_cat_data >> consume_kafka_data >> analyze_cat_data
