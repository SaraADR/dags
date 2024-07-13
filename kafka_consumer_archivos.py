import json
import os
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
    print("Esto es el mensaje")
    print(f"{message}")

    if message is not None:
        nombre = message.key()
        valor= message.value()
        Variable.set("key", nombre)
        Variable.set("value", valor)
    else:
        Variable.set("key", None)        
        Variable.set("value", None)  


    
def choose_branch(**kwargs):

    nombre_fichero = Variable.get("key")
    print(f"{nombre_fichero}")

    file_extension = os.path.splitext(nombre_fichero)[1].lower()
    print(f"{file_extension}")
    
    if file_extension == '.zip':
        return 'process_zip_task'
    elif file_extension == '.tiff' or file_extension == '.tif':
        return 'process_tiff_task'
    elif file_extension == '.jpg' or file_extension == '.jpeg':
        return 'process_jpg_task'
    elif file_extension == 'no_message_task':
        return 'no_message_task'
    else:
        return 'unknown_or_none_file_task'
    

    
def process_zip_file(**kwargs):
    try:
        value_pulled = Variable.get("value")
        print("Processing ZIP file")
    except KeyError:
        print("Variable value does not exist")
        raise AirflowSkipException("Variable value does not exist")
    Variable.set("key", None)   



def process_tiff_file(**kwargs):
    try:
        value_pulled = Variable.get("value")
        print("Processing TIFF file")
    except KeyError:
        print("Variable value does not exist")
        raise AirflowSkipException("Variable value does not exist")    
    Variable.set("key", None)   

def process_jpg_file(**kwargs):
    try:
        value_pulled = Variable.get("value")
        print("Processing JPG file")
    except KeyError:
        print("Variable value does not exist")
        raise AirflowSkipException("Variable value does not exist")
    Variable.set("key", None)   


def handle_unknown_file(**kwargs):
    print("Unknown file type")
    Variable.set("key", None)   



# def trigger_email_handler(**kwargs):
#     try:
#         value_pulled = Variable.get("my_variable_key")
#     except KeyError:
#         print("Variable my_variable_key does not exist")
#         raise AirflowSkipException("Variable my_variable_key does not exist")
    

#     if value_pulled is not None and value_pulled != 'null':
#         try:

#             trigger = TriggerDagRunOperator(
#                 task_id='trigger_email_handler_inner',
#                 trigger_dag_id='send_email_wplantilla',
#                 conf={'message': value_pulled}, 
#                 execution_date=datetime.now().replace(tzinfo=timezone.utc),
#                 dag=dag,
#             )
#             trigger.execute(context=kwargs)
#             Variable.delete("my_variable_key")
#         except json.JSONDecodeError as e:
#             print(f"Error decoding JSON: {e}")
#     else:
#         print("No message pulled from XCom")
#         Variable.delete("my_variable_key")


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
    'kafka_consumer_archivos',
    default_args=default_args,
    description='DAG que consume mensajes de Kafka y dispara otro DAG para archivos',
    schedule_interval='*/3 * * * *',
    catchup=False
)

consume_from_topic = ConsumeFromTopicOperator(
    kafka_config_id="kafka_connection",
    task_id="consume_from_topic",
    topics=["archivos"],
    apply_function=consumer_function,
    apply_function_kwargs={"prefix": "consumed:::"},
    commit_cadence="end_of_batch",
    max_messages=1,
    max_batch_size=2,
    dag=dag,
)

choose_branch_task = BranchPythonOperator(
    task_id='choose_branch_task',
    python_callable=choose_branch,
    provide_context=True,
    dag=dag,
)
process_zip_task = PythonOperator(
    task_id='process_zip_task',
    python_callable=process_zip_file,
    provide_context=True,
    dag=dag,
)

process_tiff_task = PythonOperator(
    task_id='process_tiff_task',
    python_callable=process_tiff_file,
    provide_context=True,
    dag=dag,
)

process_jpg_task = PythonOperator(
    task_id='process_jpg_task',
    python_callable=process_jpg_file,
    provide_context=True,
    dag=dag,
)

unknown_or_none_file_task = PythonOperator(
    task_id='unknown_or_none_file_task',
    python_callable=handle_unknown_file,
    provide_context=True,
    dag=dag,
)


# trigger_email_handler_task = PythonOperator(
#     task_id='trigger_email_handler',
#     python_callable=trigger_email_handler,
#     provide_context=True,
#     dag=dag,
# )


consume_from_topic >> choose_branch_task
choose_branch_task >> [process_zip_task, process_tiff_task, process_jpg_task, unknown_or_none_file_task]