import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
from airflow.providers.apache.kafka.hooks.kafka import KafkaConsumerHook
import time

def consumer_function(**kwargs):
    # Conectar con Kafka
    kafka_conn_id = "kafka_connection"
    topic = "debezium.public.notifications"  # Cambia esto al topic que necesitas
    consumer = KafkaConsumerHook(kafka_conn_id=kafka_conn_id, topics=[topic])
    
    while True:  # Loop continuo para consumir mensajes
        message_batch = consumer.get_consumer().poll(timeout_ms=1000)  # Intentar consumir mensajes
        if message_batch:
            for partition, records in message_batch.items():
                for record in records:
                    msg_value = record.value.decode('utf-8')
                    print(f"Received message: {msg_value}")
                    
                    # Procesar el mensaje
                    sendEmail(msg_value)
        else:
            print("No message received, waiting...")
            time.sleep(5)  # Esperar antes de intentar nuevamente



def sendEmail(message, **kwargs):
    try:
        print(message)

        try:
            msg_json = json.loads(message)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")


        conf = {'message': msg_json}
        trigger_dag_run = TriggerDagRunOperator(
            task_id=f'trigger_email_handler_inner',
            trigger_dag_id='send_email_plantilla',
            conf=conf,
            execution_date=datetime.now().replace(tzinfo=timezone.utc),
            dag=dag
        )
        trigger_dag_run.execute(context=kwargs)

    except Exception as e:
            print(f"Task instance incorrecto: {e}")



default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'kafka_consumer_notificaciones_dag',
    default_args=default_args,
    description='DAG que consume mensajes de Kafka y dispara otro DAG si destination=email',
    schedule_interval=None,  # Sin cron para ejecución continua
    catchup=False,
    max_active_runs=1,  # Solo una instancia activa del DAG
    is_paused_upon_creation=False,  # Inicia el DAG activo
)

# PythonOperator para consumir mensajes de Kafka
continuous_consumer_task = PythonOperator(
    task_id='continuous_kafka_consumer',
    python_callable=consumer_function,
    provide_context=True,
    dag=dag,
)


continuous_consumer_task