from datetime import timedelta
import json
import requests
from pendulum import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from kafka import KafkaConsumer

def get_a_cat_fact(ti):
    """
    Gets a cat fact from the CatFacts API
    """
    url = "http://catfact.ninja/fact"
    res = requests.get(url)
    ti.xcom_push(key="cat_fact", value=json.loads(res.text)["fact"])

def consume_from_kafka(ti):
    """
    Consumes messages from a Kafka topic and pushes them to XCom
    """
    consumer = KafkaConsumer(
        'topic1',  # Reemplaza 'your_topic' con el nombre de tu topic
        bootstrap_servers=['10.96.117.39:9092'],  # Reemplaza con la dirección de tu servidor Kafka
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='1',  # Reemplaza 'my-group' con el ID de tu grupo de consumidores
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    messages = []
    for message in consumer:
        messages.append(message.value)
        if len(messages) >= 10:  # Leemos 10 mensajes como ejemplo. Ajusta según tus necesidades
            break

    ti.xcom_push(key="message_resp", value=messages)

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

    consume_kafka_data = PythonOperator(
        task_id="consume_from_kafka",
        python_callable=consume_from_kafka,
    )

    analyze_cat_data = PythonOperator(
        task_id="analyze_data",
        python_callable=analyze_cat_facts,
    )

    get_cat_data >> consume_kafka_data >> analyze_cat_data
