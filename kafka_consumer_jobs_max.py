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
        print("Esto es el mensaje")
        print(f"{msg_value}")
        
        if msg_value:
            process_message(msg_value, kwargs)
        else:
            print("Empty message received")      
            return None  
    


def process_message(msg_value, kwargs):
    if msg_value is not None and msg_value != 'null':
        try:
            msg_json = json.loads(msg_value)
            print(msg_json)
            unique_run_id = f"manual__{datetime.utcnow().isoformat()}"

            if msg_json.get('job') == 'automaps':
                trigger = TriggerDagRunOperator(
                    task_id='trigger_automaps_handler_inner',
                    trigger_dag_id='algorithm_automaps',
                    conf={'message': msg_json}, 
                    execution_date=datetime.now().replace(tzinfo=timezone.utc),
                    dag=dag,
                )
            elif msg_json.get('job') == 'heatmap-incendios':
                trigger = TriggerDagRunOperator(
                task_id='process_heatmap_data',
                trigger_dag_id='heatmap_incendio_process',
                conf={'message': msg_json}, 
                execution_date=datetime.now().replace(tzinfo=timezone.utc),
                dag=dag
            )
            elif msg_json.get('job') == 'create_fire':
                trigger = TriggerDagRunOperator(
                task_id='trigger_fire_handler_inner',
                trigger_dag_id='create_fire',
                conf={'message': msg_json}, 
                execution_date=datetime.now().replace(tzinfo=timezone.utc),
                dag=dag
            )  
            else:
                print(f"Unrecognized job type: {msg_json.get('job')}")
                raise AirflowSkipException(f"Unrecognized job type: {msg_json.get('job')}")
            
            trigger.execute(context=kwargs)
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
    'kafka_consumer_jobs_max_dag',
    default_args=default_args,
    description='DAG que consume mensajes de la tabla de jobs',
    schedule_interval='*/1 * * * *',
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
    dag=dag
)

consume_from_topic 