import json
import logging
from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "start_date": datetime(2024, 4, 4),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


fruits_test = ["Apple", "Pear", "Peach", "Banana"]
def producer_function():
    for i in fruits_test:
        yield (json.dumps(i), json.dumps(i + i))



consumer_logger = logging.getLogger("airflow")
def consumer_function(message, prefix=None):
    key = json.loads(message.key())
    value = json.loads(message.value())
    consumer_logger.info(f"{prefix} {message.topic()} @ {message.offset()}; {key} : {value}")
    return


with DAG(
    "kafka_DAG",
    default_args=default_args,
    description="KafkaOperators",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["Test_DAG"],
) as dag:

    t1 = ProduceToTopicOperator(
        task_id="produce_to_topic",
        topic="test1",
        producer_function=producer_function,
        kafka_config={"bootstrap.servers": ":9092"},
    )


    t2 = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        topics=["topictest"],
        apply_function=consumer_function,
        apply_function_kwargs={"prefix": "consumed:::"},
        consumer_config={
            "bootstrap.servers": ":9092",
            "group.id": "1",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        },
        commit_cadence="end_of_batch",
        max_messages=10,
        max_batch_size=2,
    )