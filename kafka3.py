from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.kafka.sensors import KafkaSensor
from airflow.providers.apache.kafka.operators import KafkaConsumeOperator
from datetime import datetime

# Parámetros del DAG
topic_name = "test1"
bootstrap_servers = ["10.96.45.152:9092"]

# DAG
with DAG(
    dag_id="kafka_consume_bash",
    schedule_interval="* * * * *",
    start_date=datetime.now(),
    catchup=False,
) as dag:
    
    
    # Tarea que imprime un mensaje al iniciar
    start_message = BashOperator(
        task_id="start_message",
        bash_command="echo 'Iniciando DAG'",
    )

    # Sensor que espera un mensaje en el topic
    kafka_sensor = KafkaSensor(
        task_id="wait_for_message",
        topic=topic_name,
        bootstrap_servers=bootstrap_servers,
    )

    # Operador que consume el mensaje y lo muestra por Bash
    consume_and_print_message = KafkaConsumeOperator(
        task_id="consume_and_print_message",
        topic=topic_name,
        bootstrap_servers=bootstrap_servers,
        bash_command="echo {{ ti.message }}",
    )

    # Tarea que imprime un mensaje al finalizar
    end_message = BashOperator(
        task_id="end_message",
        bash_command="echo 'DAG finalizado'",
    )

    # Flujo del DAG
    start_message >> kafka_sensor >> consume_and_print_message >> end_message
