import ast
import base64
import io
import json
import os
import shutil
import uuid
import zipfile
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
from airflow.exceptions import AirflowSkipException
import tempfile


def consumer_function(message, prefix, **kwargs):
    print(f"Mensaje: {message}")

    try:
        msg_value = message.value().decode('utf-8')
        print("Mensaje procesado: ", msg_value)
    except Exception as e:
        print(f"El mensaje no es un texto plano: {e}")

    if message is not None:
        nombre_fichero = message.key()

        if nombre_fichero is None:
            print("El nombre del fichero es erroneo, no se puede procesar")
            return 'no_message_task'
        
        file_extension = os.path.splitext(nombre_fichero.decode('utf-8'))[1].strip().lower().replace("'", "")
        print(f"archivo: {nombre_fichero}, Extensión del archivo: {file_extension}")
        
        if file_extension == '.zip':
            process_zip_file(message.value(), nombre_fichero)
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
            process_json_file(message.value())
        elif file_extension == 'no_message_task':
            return 'no_message_task'
        else:
            return 'unknown_or_none_file_task'


def process_zip_file(value, nombre_fichero, **kwargs):
    try:
        zip_file = zipfile.ZipFile(io.BytesIO(value))
        zip_file.testzip()  
        print("El archivo ZIP es válido.")
    except zipfile.BadZipFile:
        print("El archivo no es un ZIP válido antes del procesamiento.")
        return 


    try:
        value_pulled = value
        print("Procesando ZIP")
        print(f"Tipo de value_pulled: {type(value_pulled)}")
        print(value_pulled[:4])

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_unzip_path = os.path.join(temp_dir, 'unzip')
            temp_zip_path = os.path.join(temp_dir, 'zip')

            # Crear los subdirectorios temporales
            os.makedirs(temp_unzip_path, exist_ok=True)
            os.makedirs(temp_zip_path, exist_ok=True)

            with zipfile.ZipFile(io.BytesIO(value_pulled)) as zip_file:
                
                # Obtener la lista de archivos dentro del ZIP
                file_list = zip_file.namelist()
                print("Archivos en el ZIP:", file_list)

                # Para almacenar la estructura de carpetas y archivos
                folder_structure = {}
                otros = []
                algorithm_id = None

                for file_name in file_list:
                    if file_name.endswith('/'):
                        continue

                    with zip_file.open(file_name) as file:
                        content = file.read()

                    # Determinar la carpeta a la que pertenece el archivo
                    directory = os.path.dirname(file_name)
                    if directory not in folder_structure:
                        folder_structure[directory] = []
                    folder_structure[directory].append(file_name)

                    print(file_name)


                    if os.path.basename(file_name).lower() == 'algorithm_result.json' or file_name == 'algorithm_result.json':
                        # Procesar el archivo JSON
                        json_content = json.loads(content)
                        json_content_metadata = json_content.get('metadata', [])
                        for metadata in json_content_metadata:
                            print(metadata)
                            if metadata.get('name') == 'AlgorithmID': 
                                algorithm_id = metadata.get('value')
                        print(f"algorithmId en {file_name}: {algorithm_id}")
                    else:
                        encoded_content = base64.b64encode(content).decode('utf-8')
                        otros.append({'file_name': file_name, 'content': encoded_content})


                if algorithm_id:
                    # Aquí tomas decisiones basadas en el valor de algorithmId
                    if algorithm_id == 'PowerLineVideoAnalisysRGB':
                        trigger_dag_name = 'mission_inspection_store_video_and_notification'
                        print("Ejecutando lógica para Video")

                    elif algorithm_id == 'PowerLineCloudAnalisys':   
                        trigger_dag_name = 'mission_inspection_store_cloud_and_job_update'
                        print("Ejecutando lógica para vegetacion")

                    elif algorithm_id == 'MetashapeRGB':   
                        trigger_dag_name = 'algorithm_metashape_result_upload_postprocess'
                        print("Ejecutando lógica para MetashapeRGB")


                    unique_id = uuid.uuid4()
                    if trigger_dag_name is not None:
                        try:
                            trigger = TriggerDagRunOperator(
                                task_id=str(unique_id),
                                trigger_dag_id=trigger_dag_name,
                                conf={ 'json' : json_content, 'otros' : otros}, 
                                execution_date=datetime.now().replace(tzinfo=timezone.utc),
                                dag=dag,
                            )
                            trigger.execute(context=kwargs)
                        except Exception as e:
                            print(f"Task instance incorrecto: {e}")

                else:
                    print("El archivo no contiene un algoritmo controlado")
                    return
             
    except zipfile.BadZipFile as e:
        print(f"El archivo no es un ZIP válido {e}")
        return
    
    finally:
        for temp_dir_a in temp_dir:
            if os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir_a)
                    print(f"Directorio temporal {temp_dir_a} eliminado.")
                except Exception as e:
                    print(f"Error eliminando {temp_dir}: {e}")


def process_json_file(value, **kwargs):
    try:
        value_pulled = value
        print("Processing JSON file")
        
        if not value_pulled:
            print("No data to process")
            return

        # En el caso de que el json sea visto como objeto lo limpiamos
        if isinstance(value_pulled, str) and value_pulled.startswith("b'") and value_pulled.endswith("'"):
            value_pulled = value_pulled[2:-1]
            value_pulled = value_pulled.replace('\\r', '').replace('\\n', '').strip()

        json_content = json.loads(value_pulled)
        print(f"Processed JSON content: {json_content}")

        # Agregar trigger para enviar datos al DAG 'save_coordinates_to_minio'
        trigger = TriggerDagRunOperator(
            task_id='trigger_save_coordinates',
            trigger_dag_id='save_coordinates_to_minio',
            conf={'message': json_content}, 
            execution_date=datetime.now().replace(tzinfo=timezone.utc),
            dag=dag,
        )
        trigger.execute(context=kwargs)

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        print(f"Value pulled: {value_pulled}")  
        raise AirflowSkipException("The file content is not a valid JSON")
    except KeyError:
        print("Variable value does not exist")
        raise AirflowSkipException("Variable value does not exist")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise AirflowSkipException("An unexpected error occurred")


default_args = {
    'owner': 'sadr',
    'depends_onpast': False,
    'start_date': datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}



dag = DAG(
    'kafka_consumer_archivos_max',
    default_args=default_args,
    description='DAG que consume mensajes de Kafka y dispara otro DAG para archivos',
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

consume_from_topic = ConsumeFromTopicOperator(
    kafka_config_id="kafka_connection",
    task_id="consume_from_topic",
    topics=["sftp"],
    apply_function=consumer_function,
    apply_function_kwargs={"prefix": "consumed:::"},
    commit_cadence="end_of_batch",
    max_messages=1,
    max_batch_size=1,
    dag=dag,
)


consume_from_topic