from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from datetime import datetime

with  DAG(
    dag_id="mi_dag_kafka_3",
    schedule_interval="@once",
    start_date=datetime(2024, 4, 6),
    ) as dag:

# Crear la tarea para consumir mensajes
    consume_task = ConsumeFromTopicOperator(
    task_id="consumir_mensajes_kafka",
    topics=["test1"],
    dag=dag,
)
