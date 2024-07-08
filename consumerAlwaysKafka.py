import json
from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

def consumer_function(message, prefix, **kwargs):
    if message is not None:
        msg_value = message.value().decode('utf-8')
        print(f"message2: {msg_value}")
        if msg_value:
            try:
                msg_json = json.loads(msg_value)
                if msg_json.get('destination') == 'email':
                    return msg_json
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
        else:
            print("Empty message received")
    return None

def decide_which_path(**kwargs):
    # Pull the message JSON from the XCom
    msg_json = kwargs['task_instance'].xcom_pull(task_ids='consume_from_topic')
    # Decide the next task based on whether msg_json is None or not
    if msg_json:
        return 'trigger_email_handler'
    else:
        return 'no_op'

def trigger_email_handler_dag_run(msg_json, **kwargs):
    # Logic to trigger the DAG
    if msg_json:
        print(f"Triggering email handler DAG with config: {msg_json}")
        trigger = TriggerDagRunOperator(
            task_id='trigger_email_handler_dag_run_inner',
            trigger_dag_id='recivekafka',
            conf=msg_json
        )
        trigger.execute(context=kwargs)
    else:
        print("No valid message to trigger DAG")

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
    max_batch_size=1,
    dag=dag,
)

decide_path = BranchPythonOperator(
    task_id='decide_path',
    python_callable=decide_which_path,
    provide_context=True,
    dag=dag,
)

trigger_email_handler = PythonOperator(
    task_id='trigger_email_handler',
    python_callable=trigger_email_handler_dag_run,
    provide_context=True,
    op_args=["{{ task_instance.xcom_pull(task_ids='consume_from_topic') }}"],
    dag=dag,
)

no_op = DummyOperator(
    task_id='no_op',
    dag=dag,
)

consume_from_topic >> decide_path >> [trigger_email_handler, no_op]
