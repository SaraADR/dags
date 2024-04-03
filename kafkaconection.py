from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Consumer
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def consume_messages():
    bootstrap_servers = Variable.get("kafka_bootstrap_servers")
    topic = Variable.get("kafka_topic")
    consumer_group = Variable.get("kafka_consumer_group")

    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': consumer_group,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])
    for msg in consumer:
        print(f"Received message: {msg.value().decode('utf-8')}")
        # Ejecutar una tarea de Airflow al recibir un mensaje, por ejemplo, una tarea de Bash
        bash_task.execute(context=None)




with DAG(
    'kafka_to_airflow', 
    default_args=default_args, 
    schedule_interval=None
    ) as dag:
    # Definir una tarea de Python para consumir mensajes de Kafka
    consume_kafka_task = PythonOperator(
        task_id='consume_kafka_messages',
        python_callable=consume_messages
    )

    # Definir una tarea de Bash que se ejecutará cuando se reciba un mensaje de Kafka
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "Ejecutando tarea de Airflow"'
    )

    # Establecer la dependencia entre las tareas
    consume_kafka_task >> bash_task
