import json
from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

def handle_message(message, **kwargs):
    try:
        msg_dict = json.loads(message)
    except json.JSONDecodeError:
        return None

    if msg_dict.get('destination') == 'email':
        return msg_dict
    return None

def prepare_dag_run(context, **kwargs):
    ti = context['task_instance']
    message = ti.xcom_pull(task_ids='consume_from_kafka')
    if message:
        context['task_instance'].xcom_push(key='dag_run_payload', value=message)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kafka_listener_dag',
    default_args=default_args,
    description='DAG that listens to Kafka and triggers other DAGs',
    schedule_interval='*/5 * * * *',
    catchup=False,
)

consume_task = ConsumeFromTopicOperator(
    task_id='consume_from_kafka',
    kafka_config_id="kafka_connection",
    topics=['test1'],
    apply_function='dags.kafka_listener_dag.handle_message',
    dag=dag,
)

prepare_dag_run_task = PythonOperator(
    task_id='prepare_dag_run',
    python_callable=prepare_dag_run,
    provide_context=True,
    dag=dag,
)

trigger_dag_task = TriggerDagRunOperator(
    task_id='trigger_dag_run',
    trigger_dag_id='sendmessage3',
    conf="{{ task_instance.xcom_pull(task_ids='prepare_dag_run', key='dag_run_payload') }}",
    dag=dag,
)

consume_task >> prepare_dag_run_task >> trigger_dag_task
