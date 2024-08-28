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
        nombre_fichero = message.key()
        file_extension = os.path.splitext(nombre_fichero)[1].strip().lower().replace("'", "")
        print(f"Extensión del archivo: {file_extension}")
        
        if file_extension == '.zip':
            process_zip_file(message.value())
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



def process_zip_file(value, **kwargs):
    try:
        value_pulled = value
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
                algorithm_id = None

                for file_name in file_list:
                    with zip_file.open(file_name) as file:
                        content = file.read()
                        print(f"Contenido del archivo {file_name}: {content[:10]}...")  

                    if file_name.lower().endswith(('.mp4', '.avi', '.mov', '.wmv', '.flv')):
                        videos.append(file_name)
                    elif file_name.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp')):
                        images.append(file_name)
                    elif file_name.lower().endswith('.json'):
                        # Procesar el archivo JSON
                        json_content = json.loads(content)
                        algorithm_id = json_content.get('algorithmId')
                        print(f"algorithmId en {file_name}: {algorithm_id}")

                if algorithm_id:
                    # Aquí tomas decisiones basadas en el valor de algorithmId
                    if algorithm_id == 'Video':
                        print("Ejecutando lógica para Video")
                        try:
                            # trigger = TriggerDagRunOperator(
                            #     task_id='trigger_email_handler_inner',
                            #     trigger_dag_id='save_documents_to_minio',
                            #     conf={'message': value_pulled}, 
                            #     execution_date=datetime.now().replace(tzinfo=timezone.utc),
                            #     dag=dag,
                            # )
                            # trigger.execute(context=kwargs)
                            print("Ejecutando lógica para Video")
                        except zipfile.BadZipFile:
                            print(f"Error decoding Zip")


                    elif algorithm_id == 'Vegetacion':
                        print("Ejecutando lógica para vegetacion")
                        # Lógica específica para algoritmo_2
                    # Agrega más condiciones según sea necesario

                elif videos and images is not None:
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
                    except zipfile.BadZipFile:
                        print(f"Error decoding Zip")
                elif videos and images is None:
                    print(f"No va a seguir ningun ciclo")
                       
    except KeyError:
        print("Variable value does not exist")
        raise AirflowSkipException("Variable value does not exist")
    except zipfile.BadZipFile:
        print("El archivo no es un ZIP válido")
        raise AirflowSkipException("El archivo no es un ZIP válido")



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
    'kafka_consumer_archivos_max',
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
    max_batch_size=1,
    dag=dag,
)


consume_from_topic