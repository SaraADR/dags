import ast
import base64
import io
import json
import os
import zipfile
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
from airflow.exceptions import AirflowSkipException
import tempfile



def consumer_function(message, prefix, **kwargs):
    print("Esto es el mensaje")
    print(f"{message}")

    if message is not None:
        nombre_fichero = message.key()

        if nombre_fichero is None:
            print("El nombre del fichero es None, no se puede procesar")
            return 'no_message_task'
        
        print(f"archivo: {nombre_fichero}")
        file_extension = os.path.splitext(nombre_fichero.decode('utf-8'))[1].strip().lower().replace("'", "")
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
            process_json_file(message.value())
        elif file_extension == 'no_message_task':
            return 'no_message_task'
        else:
            return 'unknown_or_none_file_task'



def process_zip_file(value, **kwargs):
    try:
        value_pulled = value
        print("Processing ZIP file")

        first_40_values = value_pulled[:20]
        print("First 20 values:", first_40_values)

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

                videos = []
                images = []
                otros = []
                algorithm_id = None

                for file_name in file_list:
                    with zip_file.open(file_name) as file:
                        content = file.read()
                        print(f"Contenido del archivo {file_name}: {content[:10]}...")  


                    # Determinar la carpeta a la que pertenece el archivo
                    directory = os.path.dirname(file_name)
                    if directory not in folder_structure:
                        folder_structure[directory] = []
                    folder_structure[directory].append(file_name)

                    print(f"{file_name}: NOMBRE")
                    if file_name.lower().endswith(('.mp4', '.avi', '.mov', '.wmv', '.flv')):
                        encoded_content = base64.b64encode(content).decode('utf-8')
                        videos.append({'file_name': file_name, 'content': encoded_content, 'directory': directory})
                    elif file_name.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp')):
                        images.append({'file_name': file_name, 'content': content, 'directory': directory})
                    elif file_name.lower() == 'algorithm_result.json':
                        # Procesar el archivo JSON
                        json_content = json.loads(content)
                        for metadata in json_content['metadata']:
                            if metadata['name'] == 'AlgorithmID':
                                algorithm_id = metadata['value']
                            break
                        print(f"algorithmId en {file_name}: {algorithm_id}")
                    else:
                        otros.append({'file_name': file_name, 'content': content, 'directory': directory})


                print("Estructura de carpetas y archivos en el ZIP:", folder_structure)
                if algorithm_id:
                    # Aquí tomas decisiones basadas en el valor de algorithmId
                    if algorithm_id == 'Video':
                        print("Ejecutando lógica para Video")
                        try:
                            trigger = TriggerDagRunOperator(
                                task_id='trigger_email_handler_inner',
                                trigger_dag_id='video',
                                conf={'videos': videos, 'images': images, 'json' : json_content, 'otros' : otros, 'structure': folder_structure}, 
                                execution_date=datetime.now().replace(tzinfo=timezone.utc),
                                dag=dag,
                            )
                            trigger.execute(context=kwargs)
                            print("Ejecutando lógica para Video")
                            return None
                        except zipfile.BadZipFile:
                            print(f"Error decoding Zip")


                    elif algorithm_id == 'Vegetacion':
                        print("Ejecutando lógica para vegetacion")
                        try:
                            trigger = TriggerDagRunOperator(
                                task_id='trigger_email_handler_inner',
                                trigger_dag_id='vegetacion',
                                conf={'videos': videos, 'images': images, 'json' : json_content, 'otros' : otros, 'structure': folder_structure}, 
                                execution_date=datetime.now().replace(tzinfo=timezone.utc),
                                dag=dag,
                            )
                            trigger.execute(context=kwargs)
                            print("Ejecutando lógica para Video")
                            return None
                        except zipfile.BadZipFile:
                            print(f"Error decoding Zip")

                    # Agrega más condiciones según sea necesario

                elif videos and images is not None:
                    try:
                        print(f"va a seguir el ciclo de detección de elementos")
                        trigger = TriggerDagRunOperator(
                            task_id='trigger_email_handler_inner',
                            trigger_dag_id='save_documents_to_minio',
                            conf={'message': value_pulled, 'structure': folder_structure}, 
                            execution_date=datetime.now().replace(tzinfo=timezone.utc),
                            dag=dag,
                        )
                        trigger.execute(context=kwargs)
                    except zipfile.BadZipFile:
                        print(f"Error decoding Zip")
                elif videos and images is None:
                    print(f"No va a seguir ningun ciclo")
                       
    except zipfile.BadZipFile:
        print("El archivo no es un ZIP válido")
        raise AirflowSkipException("El archivo no es un ZIP válido")


def process_json_file(value, **kwargs):
    try:
        value_pulled = value
        print("Processing JSON file")
        
        if not value_pulled:
            print("No data to process")
            raise AirflowSkipException("No data to process")

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
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kafka_consumer_archivos_max',
    default_args=default_args,
    description='DAG que consume mensajes de Kafka y dispara otro DAG para archivos',
    schedule_interval='*/2 * * * *',
    catchup=False
)

consume_from_topic = ConsumeFromTopicOperator(
    kafka_config_id="kafka_connection",
    task_id="consume_from_topic_sftp",
    topics=["sftp"],
    apply_function=consumer_function,
    apply_function_kwargs={"prefix": "consumed:::"},
    commit_cadence="end_of_batch",
    max_messages=1,
    max_batch_size=1,
    dag=dag,
)


consume_from_topic