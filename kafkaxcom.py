from datetime import timedelta
import json
from pendulum import datetime, duration
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator

def get_a_cat_fact(ti):
    """
    Gets a cat fact from the CatFacts API
    """
    url = "http://catfact.ninja/fact"
    res = requests.get(url)
    ti.xcom_push(key="cat_fact", value=json.loads(res.text)["fact"])

def consumer_function(message, ti):
    if message is not None:
        global message_json
        global otro_json
        otro_json = message
        message_json = json.loads(message.value().decode('utf-8'))
        # Loguear el contenido de message_json
        if message_json.get('destination') == 'email':
            ti.xcom_push(key="message_resp", value=json.loads(message))
            return True
    return False

def wrapped_consumer_function(message, **kwargs):
    ti = kwargs['ti']
    return consumer_function(message, ti)

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

    consume_task = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        topics=["test1"],
        apply_function=wrapped_consumer_function,
        kafka_config_id="kafka_connection",
        commit_cadence="end_of_batch",
        max_messages=10,
        max_batch_size=2,
    )

    analyze_cat_data = PythonOperator(
        task_id="analyze_data", 
        python_callable=analyze_cat_facts
    )

    get_cat_data >> consume_task >> analyze_cat_data
