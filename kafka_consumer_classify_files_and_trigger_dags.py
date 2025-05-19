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
from airflow.hooks.base_hook import BaseHook
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from airflow import settings
from function_save_logs_to_minio import save_logs_to_minio
from airflow.operators.python import BranchPythonOperator
from utils.log_utils import setup_conditional_log_saving
from utils.kafka_headers import extract_trace_id

from dag_utils import get_minio_client, delete_file_sftp
KAFKA_RAW_MESSAGE_PREFIX = "Mensaje crudo:"

def consumer_function(message, prefix, **kwargs):
    print(f"{KAFKA_RAW_MESSAGE_PREFIX} {message}")

    trace_id, log_msg = extract_trace_id(message)
    print(log_msg)

    try:
        msg_value = message.value().decode('utf-8')
        print("Mensaje procesado: ", msg_value)
    except Exception as e:
        print(f"Error al procesar el mensaje: {e}")
    # delete_file_sftp(msg_value)
    
    file_path_in_minio = msg_value
        
    # Establecer conexión con MinIO
    s3_client = get_minio_client()



    # Nombre del bucket donde está almacenado el archivo/carpeta
    bucket_name = 'tmp'
    folder_prefix = 'sftp/'

    # Descargar el archivo desde MinIO
    local_directory = 'temp'  # Cambia este path al local
    try:
        local_zip_path = download_from_minio(s3_client, bucket_name, file_path_in_minio, local_directory, folder_prefix)
        print(local_zip_path)
        process_zip_file(local_zip_path, file_path_in_minio, msg_value,  **kwargs)
    except Exception as e:
        print(f"Error al descargar desde MinIO: {e}")
        raise
    if msg_value:
        print("Mensaje procesado correctamente", msg_value)
        return True
    else:
        print("No se pudo procesar el mensaje")
        return False


def list_files_in_minio_folder(s3_client, bucket_name, prefix):
    """
    Lista todos los archivos dentro de un prefijo (directorio) en MinIO.
    """

    print(bucket_name),
    print(prefix)
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in response:
            print(f"No se encontraron archivos en la carpeta: {prefix}")
            return []

        files = [content['Key'] for content in response['Contents']]
        return files

    except ClientError as e:
        print(f"Error al listar archivos en MinIO: {str(e)}")
        return []


def download_from_minio(s3_client, bucket_name, file_path_in_minio, local_directory, folder_prefix):
    """
    Función para descargar archivos o carpetas desde MinIO.
    """
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    files = list_files_in_minio_folder(s3_client, bucket_name, folder_prefix)
    if not files:
        print(f"No se encontraron archivos para descargar en la carpeta: {folder_prefix}")
        return
    print(files)

    local_file = os.path.join(local_directory, os.path.basename(file_path_in_minio))
    print(f"Descargando archivo desde MinIO: {file_path_in_minio} a {local_file}")
    
    relative_path = file_path_in_minio.replace('tmp/', '')
    print("RELATIVE PATH:" + relative_path)
    try:
        # Verificar si el archivo existe antes de intentar descargarlo
        response = s3_client.get_object(Bucket=bucket_name, Key=relative_path)
        with open(local_file, 'wb') as f:
            f.write(response['Body'].read())

        print(f"Archivo descargado correctamente: {local_file}")

        return local_file
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Error 404: El archivo no fue encontrado en MinIO: {file_path_in_minio}")
        else:
            print(f"Error en el proceso: {str(e)}")
        return None  # Devolver None si hay un error


def process_zip_file(local_zip_path, nombre_fichero, message, **kwargs):

    if local_zip_path is None:
        print(f"No se pudo descargar el archivo desde MinIO: {local_zip_path}")
        return
    

    try:
        if not os.path.exists(local_zip_path):
            print(f"Archivo no encontrado: {local_zip_path}")
            return
        

        # Abre y procesa el archivo ZIP desde el sistema de archivos
        with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
            zip_file.testzip() 
            print("El archivo ZIP es válido.")
    except zipfile.BadZipFile:
        print("El archivo no es un ZIP válido antes del procesamiento.")
        return
    

    try:
        with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
            # Procesar el archivo ZIP en un directorio temporal
            with tempfile.TemporaryDirectory() as temp_dir:
                print(f"Directorio temporal creado: {temp_dir}")

                # Extraer el contenido del ZIP en el directorio temporal
                zip_file.extractall(temp_dir)

                # Obtener la lista de archivos dentro del ZIP
                file_list = zip_file.namelist()
                print("Archivos en el ZIP:", file_list)

                # Estructura para almacenar los archivos
                folder_structure = {}
                otros = []
                algorithm_id = None

                for file_name in file_list:
                    file_path = os.path.join(temp_dir, file_name)

                    if os.path.isdir(file_path):
                        # Si es un directorio, saltamos
                        continue

                    print(f"Procesando archivo: {file_name}")

                    with open(file_path, 'rb') as f:
                        content = f.read()

                    directory = os.path.dirname(file_name)
                    if directory not in folder_structure:
                        folder_structure[directory] = []
                    folder_structure[directory].append(file_name)

                    if os.path.basename(file_name).lower() == 'algorithm_result.json':
                        json_content = json.loads(content)
                        json_content_metadata = json_content.get('metadata', [])
                        for metadata in json_content_metadata:
                            if metadata.get('name') == 'AlgorithmID':
                                algorithm_id = metadata.get('value')
                        print(f"AlgorithmID encontrado en {file_name}: {algorithm_id}")
                    else:
                        if file_name.lower().endswith('.las') and "cloud-" in file_name:
                                empty_content = b''
                                encoded_empty_content = base64.b64encode(empty_content).decode('utf-8')
                                otros.append({'file_name': file_name, 'content': encoded_empty_content})
                                print(f"Archivo .las padre {file_name} procesado con 0 bytes")              
                        else:
                            encoded_content = base64.b64encode(content).decode('utf-8')
                            otros.append({'file_name': file_name, 'content': encoded_content})

                print("Estructura de carpetas y archivos en el ZIP:", folder_structure)
                # print("Archivos adicionales procesados:", otros)
                print("Archivos adicionales procesados:", [item['file_name'] for item in otros])

                # Realiza el procesamiento basado en el AlgorithmID
                if algorithm_id:
                    if algorithm_id == 'PowerLineVideoAnalisysRGB':
                        trigger_dag_name = 'mission_inspection_store_video_and_notification'
                        print("Ejecutando lógica para Video")
                    elif algorithm_id == 'PowerLineCloudAnalisys':
                        trigger_dag_name = 'mission_inspection_store_cloud_and_job_update'
                        print("Ejecutando lógica para Vegetación")
                    elif algorithm_id == 'MetashapeRGB':
                        trigger_dag_name = 'algorithm_metashape_result_upload_postprocess'
                        print("Ejecutando lógica para MetashapeRGB")
                    elif algorithm_id == 'WaterAnalysis':
                        trigger_dag_name = 'water_analysis'
                        print("Ejecutando lógica para WaterAnalysis")
                    elif algorithm_id == 'MetashapeCartografia':
                        trigger_dag_name = 'algorithm_metashape'
                        print("Ejecutando lógica para MetaShape")
                    elif algorithm_id == 'FlameFront':
                        trigger_dag_name = 'algorithm_flame_front'
                        print("Ejecutando lógica para Frente de Llamas")


                    unique_id = uuid.uuid4()
                    if trigger_dag_name and trigger_dag_name != 'water_analysis':
                        try:
                            trigger = TriggerDagRunOperator(
                                task_id=str(unique_id),
                                trigger_dag_id=trigger_dag_name,
                                conf={'json': json_content, 'otros': otros},
                                execution_date=datetime.now().replace(tzinfo=timezone.utc),
                                dag=kwargs.get('dag'),
                            )
                            trigger.execute(context=kwargs)
                        except Exception as e:
                            print(f"Error al desencadenar el DAG: {e}")
                    elif trigger_dag_name and trigger_dag_name == 'water_analysis':
                        try:
                            trigger = TriggerDagRunOperator(
                                task_id=str(unique_id),
                                trigger_dag_id=trigger_dag_name,
                                conf={'json': json_content, 'otros': message},
                                execution_date=datetime.now().replace(tzinfo=timezone.utc),
                                dag=kwargs.get('dag'),
                            )
                            trigger.execute(context=kwargs)
                        except Exception as e:
                            print(f"Error al desencadenar el DAG: {e}")
                else:
                    unique_id = uuid.uuid4()
                    trigger_dag_name = 'zips_no_algoritmos'
                    if trigger_dag_name:
                        try:
                            trigger = TriggerDagRunOperator(
                                task_id=str(unique_id),
                                trigger_dag_id=trigger_dag_name,
                                conf={'minio': message},
                                execution_date=datetime.now().replace(tzinfo=timezone.utc),
                                dag=kwargs.get('dag'),
                            )
                            trigger.execute(context=kwargs)
                        except Exception as e:
                            print(f"Error al desencadenar el DAG: {e}")
                    print("Advertencia: No se encontró AlgorithmID en el archivo ZIP.")
                    return
    except zipfile.BadZipFile as e:
        print(f"El archivo no es un ZIP válido: {e}")
        return
    
def there_was_kafka_message(**context):
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    task_id = 'consume_from_topic_minio'
    log_base = "/opt/airflow/logs"
    log_path = f"{log_base}/dag_id={dag_id}/run_id={run_id}/task_id={task_id}"
    
    # Search for the latest log file
    try:
        latest_log = max(
            (os.path.join(root, f) for root, _, files in os.walk(log_path) for f in files),
            key=os.path.getctime
        )
        with open(latest_log, 'r') as f:
            content = f.read()
            return f"{KAFKA_RAW_MESSAGE_PREFIX} <cimpl.Message object at" in content
    except (ValueError, FileNotFoundError):
        return False

default_args = {
    'owner': 'sadr',
    'depends_onpast': False,
    'start_date': datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'kafka_consumer_classify_files_and_trigger_dags',
    default_args=default_args,
    description='DAG que consume mensajes de Kafka y dispara otro DAG para archivos',
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

consume_from_topic = ConsumeFromTopicOperator(
    kafka_config_id="kafka_connection",
    task_id="consume_from_topic_minio",
    topics=["fileV1"],
    apply_function=consumer_function,
    apply_function_kwargs={"prefix": "consumed:::"},
    commit_cadence="end_of_batch",
    dag=dag,
)

from utils.log_utils import setup_conditional_log_saving

check_logs, save_logs = setup_conditional_log_saving(
    dag=dag,
    task_id='save_logs_to_minio',
    task_id_to_save='consume_from_topic_minio',
    condition_function=there_was_kafka_message
)

consume_from_topic >> check_logs