import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone


def consumer_function(message, prefix, **kwargs):
    if message is not None:
        msg_value = message.value().decode('utf-8')
        print(f"message: {msg_value}")
        
        if msg_value:
            sendEmail(msg_value)
        else:
            print("Empty message received")    
            return None 
    else:
        print("Empty message received")    
        return None  
         



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
    schedule_interval='*/3 * * * *',
    catchup=False
)

consume_from_topic = ConsumeFromTopicOperator(
    kafka_config_id="kafka_connection",
    task_id="consume_from_topic",
    topics=["debezium.public.notifications"],
    apply_function=consumer_function,
    apply_function_kwargs={"prefix": "consumed:::"},
    commit_cadence="end_of_batch",
    max_messages=1,
    max_batch_size=2,
    dag=dag,
)


consume_from_topic 