import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.models import Variable

def consumer_function(message, prefix, **kwargs):
    if message is not None:
        msg_value = message.value().decode('utf-8')
        print(f"message2: {msg_value}")
        if msg_value:
            try:
                msg_json = json.loads(msg_value)
                if msg_json.get('destination') == 'email' and msg_json.get('status') == 'pending':
                    Variable.set("my_variable_key", message)
                    return msg_json  # Returning msg_json to be pushed to XCom
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
        else:
            print("Empty message received")
    Variable.set("my_variable_key", message)        
    return None  # Returning None if message is empty or not valid

def trigger_email_handler(**kwargs):
    value_pulled = Variable.get("my_variable_key")
    print(f"messageTRAS TRIGg: {value_pulled}")
    if value_pulled is not None:
        msg_json = json.loads(value_pulled.value().decode('utf-8'))
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
    max_messages=1,
    max_batch_size=2,
    dag=dag,
)

trigger_email_handler_task = PythonOperator(
    task_id='trigger_email_handler',
    python_callable=trigger_email_handler,
    provide_context=True,
    dag=dag,
)


consume_from_topic >> trigger_email_handler_task
