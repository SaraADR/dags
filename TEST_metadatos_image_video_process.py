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
    messages = []
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                print("No hay más mensajes que leer en el topic")
                break  
            if msg.error():
                raise KafkaException(msg.error())
            else:
                decoded_msg = msg.value().decode('utf-8')
                print(f"Received message: {decoded_msg}")
                messages.append(decoded_msg)

        
        if messages:
            print(f"Total messages received: {len(messages)}")
            consumer.commit()

    finally:
        consumer.close()



default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'TEST_metadatos_image_video_process',
    default_args=default_args,
    description='DAG ',
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1
    
)


poll_task = PythonOperator(
        task_id='poll_kafka',
        python_callable=poll_kafka_messages,
        provide_context=True,
        dag=dag
)

poll_task