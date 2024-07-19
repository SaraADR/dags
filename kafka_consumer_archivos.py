import ast
import io
import json
import os
import zipfile
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
import tempfile


def consumer_function(message, prefix, **kwargs):
    print("Esto es el mensaje")
    print(f"{message}")

    if message is not None:
        nombre = message.key()
        valor = message.value()
        Variable.set("key", nombre)
        Variable.set("value", valor)
    else:
        Variable.set("key", None)        
        Variable.set("value", None)  


def choose_branch(**kwargs):
    nombre_fichero = Variable.get("key")
    print(f"{nombre_fichero}")

    file_extension = os.path.splitext(nombre_fichero)[1].strip().lower().replace("'", "")
    print(f"Extensión del archivo: {file_extension}")

    if file_extension == '.zip':
        return 'process_zip_task'
    elif file_extension == '.tiff' or file_extension == '.tif':
        return 'process_tiff_task'
    elif file_extension == '.jpg' or file_extension == '.jpeg':
        return 'process_jpg_task'
    elif file_extension == '.png':
        return 'process_png_task'
    elif file_extension == '.mp4' or file_extension == '.avi' or file_extension == '.mov':
        return 'process_video_task'
    elif file_extension == '.json':
        return 'process_json_task'
    elif file_extension == 'no_message_task':
        return 'no_message_task'
    else:
        return 'unknown_or_none_file_task'


def process_zip_file(**kwargs):
    try:
        value_pulled = Variable.get("value")
        print("Processing ZIP file")
        first_40_values = value_pulled[:40]
        print("First 40 values:", first_40_values)

        message_dict = ast.literal_eval(value_pulled)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_unzip_path = os.path.join(temp_dir, 'unzip')
            temp_zip_path = os.path.join(temp_dir, 'zip')

            # Crear los subdirectorios temporales
            os.makedirs(temp_unzip_path, exist_ok=True)
            os.makedirs(temp_zip_path, exist_ok=True)

            with zipfile.ZipFile(io.BytesIO(message_dict)) as zip_file:
                # Obtener la lista de archivos dentro del ZIP
                file_list = zip_file.namelist()
                print("Archivos en el ZIP:", file_list)

                videos = []
                images = []

                for file_name in file_list:
                    with zip_file.open(file_name) as file:
                        content = file.read()
                        print(f"Contenido del archivo {file_name}: {content[:10]}...")  

                    if file_name.lower().endswith(('.mp4', '.avi', '.mov', '.wmv', '.flv')):
                        videos.append(file_name)
                    elif file_name.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp')):
                        images.append(file_name)

                if videos and images is not None:
                    try:
                        print(f"va a seguir el ciclo de detección de elementos")
                        trigger = TriggerDagRunOperator(
                            task_id='trigger_email_handler_inner',
                            trigger_dag_id='save_documents_to_minio',
                            conf={'message': value_pulled}, 
                            execution_date=datetime.now().replace(tzinfo=timezone.utc),
                            dag=dag,
                        )
                        trigger.execute(context=kwargs)
                        Variable.delete("value")
                    except zipfile.BadZipFile:
                        print(f"Error decoding Zip")
                if videos and images is None:
                    print(f"No va a seguir ningun ciclo")
                       
    except KeyError:
        print("Variable value does not exist")
        raise AirflowSkipException("Variable value does not exist")
    except zipfile.BadZipFile:
        print("El archivo no es un ZIP válido")
        raise AirflowSkipException("El archivo no es un ZIP válido")
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

def process_png_file(**kwargs):
    try:
        value_pulled = Variable.get("value")
        print("Processing PNG file")
    except KeyError:
        print("Variable value does not exist")
        raise AirflowSkipException("Variable value does not exist")
    Variable.set("key", None)   

def process_video_file(**kwargs):
    try:
        value_pulled = Variable.get("value")
        print("Processing Video file")
    except KeyError:
        print("Variable value does not exist")
        raise AirflowSkipException("Variable value does not exist")
    Variable.set("key", None)   

def handle_unknown_file(**kwargs):
    print("Unknown file type")
    Variable.set("key", None)   

import json

def process_json_file(**kwargs):
    try:
        value_pulled = Variable.get("value")
        print("Processing JSON file")
        
        # Verificar el tipo de dato
        if isinstance(value_pulled, bytes):
            value_pulled = value_pulled.decode('utf-8').strip()  # Decodificar y eliminar espacios adicionales

        print(f"Content of JSON file: {value_pulled}")

        # Intentar cargar el JSON
        try:
            json_content = json.loads(value_pulled)
            print(f"Processed JSON content: {json_content}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            raise AirflowSkipException("The file content is not a valid JSON")

    except KeyError:
        print("Variable value does not exist")
        raise AirflowSkipException("Variable value does not exist")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise AirflowSkipException("An unexpected error occurred")
    finally:
        Variable.set("key", None)



default_args = {
    'owner': 'airflow',
    'depends_onpast': False,
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

process_png_task = PythonOperator(
    task_id='process_png_task',
    python_callable=process_png_file,
    provide_context=True,
    dag=dag,
)

process_video_task = PythonOperator(
    task_id='process_video_task',
    python_callable=process_video_file,
    provide_context=True,
    dag=dag,
)

process_json_task = PythonOperator(
    task_id='process_json_task',
    python_callable=process_json_file,
    provide_context=True,
    dag=dag,
)

unknown_or_none_file_task = PythonOperator(
    task_id='unknown_or_none_file_task',
    python_callable=handle_unknown_file,
    provide_context=True,
    dag=dag,
)

consume_from_topic >> choose_branch_task
choose_branch_task >> [
    process_zip_task,
    process_tiff_task,
    process_jpg_task,
    process_png_task,
    process_video_task,
    process_json_task,
    unknown_or_none_file_task
]
