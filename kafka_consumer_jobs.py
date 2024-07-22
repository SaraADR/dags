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
                Variable.set("mensaje_save", msg_json)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
        else:
            print("Empty message received")
        Variable.set("mensaje_save", None)        
        return None  
    else:
        Variable.set("mensaje_save", None)        

def trigger_email_handler(**kwargs):
    try:
        value_pulled = Variable.get("mensaje_save")
        print(f"pulled {value_pulled}")
    except KeyError:
        print("Variable mensaje_save does not exist")
        raise AirflowSkipException("Variable mensaje_save does not exist")
    

    if value_pulled is not None and value_pulled != 'null':
        try:
            msg_json = json.loads(value_pulled)
            print(msg_json)
            if msg_json.get('jobs') == 'automaps':
                trigger = TriggerDagRunOperator(
                task_id='trigger_email_handler_inner',
                trigger_dag_id='algorithm_automaps',
                conf={'message': value_pulled}, 
                execution_date=datetime.now().replace(tzinfo=timezone.utc),
                dag=dag,
            )
            trigger.execute(context=kwargs)
            Variable.delete("mensaje_save")
            
            if msg_json.get('jobs') == 'create_fire':
                trigger = TriggerDagRunOperator(
                task_id='trigger_email_handler_inner',
                trigger_dag_id='create_fire',
                conf={'message': value_pulled}, 
                execution_date=datetime.now().replace(tzinfo=timezone.utc),
                dag=dag,
            )
            trigger.execute(context=kwargs)
            Variable.delete("mensaje_save")
            
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
    else:
        print("No message pulled from XCom")
        Variable.delete("mensaje_save")


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
    'kafka_consumer_jobs_dag',
    default_args=default_args,
    description='DAG que consume mensajes de la tabla de jobs',
    schedule_interval='*/3 * * * *',
    catchup=False
)

consume_from_topic = ConsumeFromTopicOperator(
    kafka_config_id="kafka_connection",
    task_id="consume_from_topic",
    topics=["jobs"],
    apply_function=consumer_function,
    apply_function_kwargs={"prefix": "consumed:::"},
    commit_cadence="end_of_batch",
    max_messages=1,
    max_batch_size=1,
    dag=dag,
)

trigger_email_handler_task = PythonOperator(
    task_id='trigger_email_handler',
    python_callable=trigger_email_handler,
    provide_context=True,
    dag=dag,
)


consume_from_topic >> trigger_email_handler_task