from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def consumer_function(message, prefix, **kwargs):
    msg_value = message.value().decode('utf-8')
    msg_json = json.loads(msg_value)
    if msg_json.get('destination') == 'email':
        ti = kwargs['ti']
        ti.xcom_push(key='email_message', value=msg_json)

def trigger_email_handler_dag(**kwargs):
    message = kwargs['ti'].xcom_pull(task_ids='consume_from_topic', key='email_message')
    if message:
        trigger_dag_run = TriggerDagRunOperator(
            task_id='trigger_email_handler',
            trigger_dag_id='sendmessage3',
            conf=message
        )
        trigger_dag_run.execute(context=kwargs)

dag = DAG(
    'kafka_consumer_trigger_dag',
    default_args=default_args,
    description='DAG que consume mensajes de Kafka y dispara otro DAG si destination=email',
    schedule_interval='*/2 * * * *',
    catchup=False
)

consume_from_topic = ConsumeFromTopicOperator(
    kafka_config_id="kafka_connection",
    task_id="consume_from_topic",
    topics=["test_1"],
    apply_function=consumer_function,
    apply_function_kwargs={"prefix": "consumed:::"},
    commit_cadence="end_of_batch",
    max_messages=10,
    max_batch_size=2,
    dag=dag,
)

trigger_email_handler = PythonOperator(
    task_id='trigger_email_handler',
    python_callable=trigger_email_handler_dag,
    provide_context=True,
    dag=dag,
)

consume_from_topic >> trigger_email_handler
