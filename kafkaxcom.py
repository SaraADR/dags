from datetime import timedelta
import json
from pendulum import datetime, duration
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.hooks.base_hook import BaseHook
from confluent_kafka import Consumer, KafkaException

def get_a_cat_fact(ti):
    """
    Gets a cat fact from the CatFacts API
    """
    url = "http://catfact.ninja/fact"
    res = requests.get(url)
    ti.xcom_push(key="cat_fact", value=json.loads(res.text)["fact"])

def consume_messages():
    # Obtener la conexión de Kafka desde Airflow
    kafka_conn = BaseHook.get_connection('kafka_connection')
    
    conf = {
        'bootstrap.servers': f'{kafka_conn.host}:{kafka_conn.port}',
        'group.id': '1',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'PLAINTEXT'
    }
    
    if kafka_conn.login:
        conf['sasl.username'] = kafka_conn.login
        conf['sasl.password'] = kafka_conn.password
        conf['security.protocol'] = 'SASL_PLAINTEXT'
        conf['sasl.mechanisms'] = 'PLAIN'

    consumer = Consumer(conf)
    topic = 'my_topic'
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code():
                    continue
                else:
                    raise KafkaException(msg.error())
            print(f'Received message: {msg.value().decode("utf-8")}')
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def analyze_cat_facts(ti):
    """
    Prints the cat fact
    """
    cat_fact = ti.xcom_pull(key="cat_fact", task_ids="get_a_cat_fact")
    print("Cat fact for today:", cat_fact)

    message_fact = ti.xcom_pull(key="message_resp", task_ids="consume_from_topic")
    print("Message fact:", message_fact)
    # run some analysis here

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
    description="arg",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    get_cat_data = PythonOperator(
        task_id="get_a_cat_fact", 
        python_callable=get_a_cat_fact
    )

    # Definir el PythonOperator
    consume_task = PythonOperator(
        task_id='consume_kafka_messages',
        python_callable=consume_messages,
    )

    analyze_cat_data = PythonOperator(
        task_id="analyze_data", 
        python_callable=analyze_cat_facts
    )

    get_cat_data >> consume_task >> analyze_cat_data
