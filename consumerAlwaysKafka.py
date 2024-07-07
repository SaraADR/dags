import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.api.common.experimental.trigger_dag import trigger_dag
from datetime import datetime, timedelta


def handle_message(message, **kwargs):
    ti = kwargs.get('ti')  # Obtener 'ti' desde kwargs

    if not message:
        return None

    try:
        msg_dict = json.loads(message.value())
    except json.JSONDecodeError:
        return None

    if msg_dict.get('destination') == 'email':
        if ti is not None:
            ti.xcom_push(key='message', value=msg_dict)  # Usar ti para push a XCom
        else:
            raise ValueError("Task Instance (ti) is None. Cannot push message to XCom.")

        return msg_dict

    return None

def trigger_sendmessage3_dag(**kwargs):
    message = kwargs['ti'].xcom_pull(task_ids='consume_from_kafka', key='message')
    if message:
        trigger_dag(dag_id='sendmessage3', run_id=None, conf=message)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
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
    apply_function=handle_message,  # Referencia directa a la función definida en el mismo archivo
    dag=dag,
    provide_context=True
)

trigger_dag_task = PythonOperator(
    task_id='trigger_sendmessage3_dag',
    provide_context=True,
    python_callable=trigger_sendmessage3_dag,
    dag=dag,
)

consume_task >> trigger_dag_task
