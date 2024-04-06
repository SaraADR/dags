from airflow import DAG
from airflow.operators.kafka import KafkaOperator
from datetime import datetime

dag = DAG(
    dag_id="mi_dag_kafka_3",
    schedule_interval="@once",
    start_date=datetime(2024, 4, 6),
)

# Crear la tarea para consumir mensajes
consume_task = KafkaOperator(
    task_id="consumir_mensajes_kafka",
    topic="test1",
    bootstrap_servers="10.96.45.152:9092",
    group_id="1",
    dag=dag,
)

# Iniciar el DAG
dag.run()