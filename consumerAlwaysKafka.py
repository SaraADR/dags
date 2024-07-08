import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta

def consumer_function(message, prefix, **kwargs):
    
    if message is not None:
        msg_value = message.value().decode('utf-8')
        print(f"message2: {msg_value}")
        if msg_value:
            try:
                msg_json = json.loads(msg_value)
                if msg_json.get('destination') == 'email' and msg_json.get('status') == 'pending':
                    decide_which_path(msg_json)
                    return msg_json  # Returning msg_json to be pushed to XCom
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
        else:
            print("Empty message received")
    return None  # Returning None if message is empty or not valid

def decide_which_path(dato, **kwargs):
    ti = kwargs.get('ti')
    msg_json = ti.xcom_push(key='message', value=dato)
    print(f"Que trae: {msg_json}")
    if msg_json:
        return 'trigger_email_handler'
    else:
        return 'no_op'

def trigger_email_handler(**kwargs):
    ti = kwargs['task_instance']
    msg_json = ti.xcom_pull(task_ids='consume_from_topic')
    if msg_json:
        trigger = TriggerDagRunOperator(
            task_id='trigger_email_handler_inner',
            trigger_dag_id='recivekafka',
            conf=msg_json,
        )
        trigger.execute(context=kwargs)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'kafka_consumer_trigger_dag',
    default_args=default_args,
    description='DAG que consume mensajes de Kafka y dispara otro DAG si destination=email',
    schedule_interval='*/5 * * * *',
    catchup=False
)

consume_from_topic = ConsumeFromTopicOperator(
    kafka_config_id="kafka_connection",
    task_id="consume_from_topic",
    topics=["test1"],
    apply_function=consumer_function,
    apply_function_kwargs={"prefix": "consumed:::"},
    commit_cadence="end_of_batch",
    max_messages=10,
    max_batch_size=2,
    dag=dag,
)

decide_path = BranchPythonOperator(
    task_id='decide_path',
    python_callable=decide_which_path,
    provide_context=True,
    dag=dag,
)

trigger_email_handler_task = PythonOperator(
    task_id='trigger_email_handler',
    python_callable=trigger_email_handler,
    provide_context=True,
    dag=dag,
)

no_op = DummyOperator(
    task_id='no_op',
    dag=dag,
)

consume_from_topic >>  [trigger_email_handler_task, no_op]
