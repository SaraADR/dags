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
            return msg_value
        else:
            print("Empty message received")      
            return None  
    


def trigger_dag_run(msg_value, **kwargs):
    msg_value = kwargs['ti'].xcom_pull(task_ids='consume_from_topic')
    if msg_value is not None and msg_value != 'null':
        try:
            msg_json = json.loads(msg_value)
            print(msg_json)
            unique_run_id = f"manual__{datetime.utcnow().isoformat()}"

            job = msg_json.get('job')
            conf = {'message': msg_json}
            
            if job == 'automaps':
                dag_to_trigger = 'algorithm_automaps'
            elif job == 'heatmap-incendios':
                dag_to_trigger = 'heatmap_incendio_process'
            elif job == 'create_fire':
                dag_to_trigger = 'create_fire'
            else:
                print(f"Unrecognized job type: {job}")
                raise AirflowSkipException(f"Unrecognized job type: {job}")

            trigger_dag_run = TriggerDagRunOperator(
                task_id=f'trigger_{job}_handler',
                trigger_dag_id=dag_to_trigger,
                conf=conf,
                execution_date=datetime.now().replace(tzinfo=timezone.utc),
                dag=kwargs['dag']
            )
            trigger_dag_run.execute(kwargs)

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

trigger_dag_task = PythonOperator(
    task_id='trigger_dag_run',
    python_callable=trigger_dag_run,
    provide_context=True,
    dag=dag,
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

consume_from_topic >> trigger_dag_task