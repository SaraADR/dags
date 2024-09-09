import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

def consumer_function(message, prefix, **kwargs):
    if message is not None:
        msg_value = message.value().decode('utf-8')
        print(f"message2: {msg_value}")
        if msg_value:
            try:
                msg_json = json.loads(msg_value)
                if msg_json.get('destination') == 'ignis' and msg_json.get('status') == 'pending':
                    Variable.set("my_variable_key", msg_json)
                    return msg_json 
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
        else:
            print("Empty message received")
        Variable.set("my_variable_key", None)        
        return None  
    else:
        Variable.set("my_variable_key", None)        

def trigger_email_handler(**kwargs):
    try:
        value_pulled = Variable.get("my_variable_key")
    except KeyError:
        print("Variable my_variable_key does not exist")
        raise AirflowSkipException("Variable my_variable_key does not exist")
    

    if value_pulled is not None and value_pulled != 'null':
        try:

            trigger = TriggerDagRunOperator(
                task_id='trigger_email_handler_inner',
                trigger_dag_id='send_email_plantilla',
                conf={'message': value_pulled}, 
                execution_date=datetime.now().replace(tzinfo=timezone.utc),
                dag=dag,
            )
            trigger.execute(context=kwargs)
            Variable.delete("my_variable_key")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
    else:
        print("No message pulled from XCom")
        Variable.delete("my_variable_key")


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
    'kafka_consumer_trigger_dag',
    default_args=default_args,
    description='DAG que consume mensajes de Kafka y dispara otro DAG si destination=email',
    schedule_interval='*/3 * * * *',
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