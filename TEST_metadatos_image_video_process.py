from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Consumer, KafkaException

def poll_kafka_messages(**context):
    conf = {
        'bootstrap.servers': '10.96.180.179:9092',
        'group.id': '1',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    consumer = Consumer(conf)
    consumer.subscribe(['metadata11'])

    try:
        messages = []
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break  # No más mensajes
            if msg.error():
                raise KafkaException(msg.error())
            else:
                decoded_msg = msg.value().decode('utf-8')
                print(f"Received message: {decoded_msg}")
                messages.append(decoded_msg)

        # Procesa los mensajes aquí si quieres
        if messages:
            print(f"Total messages received: {len(messages)}")

    finally:
        consumer.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='kafka_polling_dag',
    default_args=default_args,
    schedule_interval='*/1 * * * *',  # Cada minuto
    catchup=False,
) as dag:

    poll_task = PythonOperator(
        task_id='poll_kafka',
        python_callable=poll_kafka_messages,
        provide_context=True
    )