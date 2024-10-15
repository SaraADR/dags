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

def consumer_function(message, prefix, **kwargs):
    print(f"Mensaje crudo: {message}")
    try:
        msg_value = message.value().decode('utf-8')
        print("Mensaje procesado: ", msg_value)
    except Exception as e:
        print(f"Error al procesar el mensaje: {e}")

    
    file_path_in_minio = msg_value  # path que recibes de Kafka
        
    # Establecer conexión con MinIO
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

        # Nombre del bucket donde está almacenado el archivo/carpeta
    bucket_name = 'temp'

    # Descargar el archivo desde MinIO
    local_directory = 'tmp'  # Cambia este path al local
    local_zip_path = download_from_minio(s3_client, bucket_name, file_path_in_minio, local_directory)
    process_zip_file(local_zip_path, file_path_in_minio, **kwargs)



def download_from_minio(s3_client, bucket_name, file_path_in_minio, local_directory):
    """
    Función para descargar archivos o carpetas desde MinIO.
    """
    # Crear el directorio local si no existe
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    local_file = os.path.join(local_directory, os.path.basename(file_path_in_minio))
    print(f"Descargando archivo desde MinIO: {file_path_in_minio} a {local_file}")
    s3_client.download_file(Bucket=bucket_name, Key=file_path_in_minio, Filename=local_file)

    return local_file


def process_zip_file(local_zip_path, nombre_fichero, **kwargs):
    try:
        # Abre y procesa el archivo ZIP desde el sistema de archivos
        with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
            zip_file.testzip()  # Verifica la integridad del ZIP
            print("El archivo ZIP es válido.")
    except zipfile.BadZipFile:
        print("El archivo no es un ZIP válido antes del procesamiento.")
        return

    try:
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
                    encoded_content = base64.b64encode(content).decode('utf-8')
                    otros.append({'file_name': file_name, 'content': encoded_content})

            print("Estructura de carpetas y archivos en el ZIP:", folder_structure)
            print("Archivos adicionales procesados:", otros)

            # Realiza el procesamiento basado en el AlgorithmID
            if algorithm_id:
                if algorithm_id == 'PowerLineVideoAnalisysRGB':
                    trigger_dag_name = 'video'
                    print("Ejecutando lógica para Video")
                elif algorithm_id == 'PowerLineCloudAnalisys':
                    trigger_dag_name = 'vegetacion'
                    print("Ejecutando lógica para Vegetación")
                elif algorithm_id == 'MetashapeRGB':
                    trigger_dag_name = 'metashape_rgb'
                    print("Ejecutando lógica para MetashapeRGB")

                unique_id = uuid.uuid4()
                if trigger_dag_name:
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
            else:
                print("Advertencia: No se encontró AlgorithmID en el archivo ZIP.")
                raise AirflowSkipException("El archivo no contiene un algoritmo controlado")
    except zipfile.BadZipFile as e:
        print(f"El archivo no es un ZIP válido: {e}")
        raise AirflowSkipException("El archivo no es un ZIP válido")


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
    'kafka_consumer_archivos_max_minio',
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
    topics=["minio"],
    max_messages=1,
    max_batch_size = 1,
    apply_function=consumer_function,
    apply_function_kwargs={"prefix": "consumed:::"},
    commit_cadence="end_of_batch",
    dag=dag,
)


consume_from_topic