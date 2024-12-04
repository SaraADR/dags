import json
import uuid
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
        print("Mensaje consumido: ")
        print(f"{msg_value}")
        
        if msg_value:
            process_message(msg_value)
        else:
            print("Empty message received")      
            return None  
    else:
        print("Empty message received")    
        return None  

# process rabbit msg
def process_message(message, **kwargs):
    try:
        try:
            msg_json = json.loads(message)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")

        unique_id = uuid.uuid4()
        trigger_dag_run = TriggerDagRunOperator(
            task_id=str(unique_id),
            trigger_dag_id='function_create_fire_from_rabbit',
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
    'kafka_consumer_rabbit_avincis',
    default_args=default_args,
    description='DAG que consume eventos de la cola rabbit de avincis (creacion/edicion de incendios - fires) (viene: nifi --> kafka --> DAG)',
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1    
)

consume_from_topic = ConsumeFromTopicOperator(
    kafka_config_id="kafka_connection",
    task_id="consume_from_topic",
    topics=["rabbit_einforex"],
    apply_function=consumer_function,
    apply_function_kwargs={"prefix": "consumed:::"},
    commit_cadence="end_of_batch",
    dag=dag,
)

consume_from_topic 