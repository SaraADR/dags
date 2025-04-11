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

from dag_utils import get_minio_client, delete_file_sftp

def consumer_function(message, prefix, **kwargs):
    print(f"Mensaje crudo: {message}")

    trace_id = None

    if hasattr(message, 'headers') and callable(message.headers):
        headers = message.headers()
        print(f"Headers (contenido): {headers}")
        
        if headers:
            for key, value in headers:
                # Convertir bytes a string
                key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                value_str = value.decode('utf-8') if isinstance(value, bytes) else value
                
                print(f"Header: {key_str} = {value_str}")
                
                if key_str == 'traceId':
                    trace_id = value_str

    if trace_id is None:
        print("No se encontró traceId en los headers")
        # Generar ID si no existe
        trace_id = f"airflow-generated_{uuid.uuid4()}"
    else:
        print(f"TraceId encontrado: {trace_id}")

    try:
        msg_value = message.value().decode('utf-8')
        print("Mensaje procesado: ", msg_value)
    except Exception as e:
        print(f"Error al procesar el mensaje: {e}")
    
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
        
        # En lugar de intentar ejecutar el trigger aquí, guardamos la información necesaria en XCom
        result = analyze_zip_file(local_zip_path, file_path_in_minio, **kwargs)
        
        # Guardar información en XCom
        ti = kwargs.get('ti')
        if ti:
            ti.xcom_push(key='trigger_info', value=result)
            ti.xcom_push(key='trace_id', value=trace_id)
        
        return result
    except Exception as e:
        print(f"Error al descargar desde MinIO: {e}")
        raise


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


def analyze_zip_file(local_zip_path, file_path_in_minio, **kwargs):
    """
    Analiza el contenido del ZIP y devuelve información para el trigger.
    No ejecuta el trigger directamente.
    """
    if local_zip_path is None:
        print(f"No se pudo descargar el archivo desde MinIO: {local_zip_path}")
        return None
    
    try:
        if not os.path.exists(local_zip_path):
            print(f"Archivo no encontrado: {local_zip_path}")
            return None
        
        # Abre y procesa el archivo ZIP desde el sistema de archivos
        with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
            zip_file.testzip() 
            print("El archivo ZIP es válido.")
    except zipfile.BadZipFile:
        print("El archivo no es un ZIP válido antes del procesamiento.")
        return None
    
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
                json_content = None

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
                print("Archivos adicionales procesados:", len(otros))

                # Determinar qué DAG debe ser desencadenado
                trigger_dag_name = None
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
                else:
                    trigger_dag_name = 'zips_no_algoritmos'
                    print("No se encontró AlgorithmID en el archivo ZIP.")

                # Devolver la información necesaria para el trigger
                return {
                    'trigger_dag_name': trigger_dag_name,
                    'algorithm_id': algorithm_id,
                    'json_content': json_content,
                    'otros': otros,
                    'file_path_in_minio': file_path_in_minio
                }
    except zipfile.BadZipFile as e:
        print(f"El archivo no es un ZIP válido: {e}")
        return None


def determine_dag_to_trigger(**context):
    """
    Determina qué DAG debe ser desencadenado basado en los datos del XCom.
    """
    ti = context['ti']
    trigger_info = ti.xcom_pull(task_ids='consume_from_topic_minio', key='trigger_info')
    
    if not trigger_info:
        print("No hay información de trigger disponible.")
        return None
    
    trigger_dag_name = trigger_info.get('trigger_dag_name')
    algorithm_id = trigger_info.get('algorithm_id')
    
    print(f"DAG a desencadenar: {trigger_dag_name}, AlgorithmID: {algorithm_id}")
    
    return {
        'trigger_dag_name': trigger_dag_name,
        'conf': {
            'json': trigger_info.get('json_content'),
            'otros': trigger_info.get('otros'),
            'minio': trigger_info.get('file_path_in_minio')
        }
    }


def trigger_appropriate_dag(**context):
    """
    Desencadena el DAG apropiado basado en la información del XCom.
    """
    ti = context['ti']
    dag_info = ti.xcom_pull(task_ids='determine_dag_to_trigger')
    
    if not dag_info or not dag_info.get('trigger_dag_name'):
        print("No hay información suficiente para desencadenar un DAG.")
        return
    
    trigger_dag_name = dag_info['trigger_dag_name']
    conf = dag_info['conf']
    
    print(f"Desencadenando DAG: {trigger_dag_name}")
    
    # Aquí usamos el TriggerDagRunOperator directamente dentro de la tarea
    trigger = TriggerDagRunOperator(
        task_id=f"trigger_{trigger_dag_name}",
        trigger_dag_id=trigger_dag_name,
        conf=conf,
        dag=context['dag']
    )
    
    trigger.execute(context=context)


default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
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
    topics=["files"],
    apply_function=consumer_function,
    apply_function_kwargs={"prefix": "consumed:::"},
    commit_cadence="end_of_batch",
    provide_context=True,  # Importante para que kwargs tenga 'ti'
    dag=dag,
)

determine_dag = PythonOperator(
    task_id='determine_dag_to_trigger',
    python_callable=determine_dag_to_trigger,
    provide_context=True,
    dag=dag
)

trigger_dag = PythonOperator(
    task_id='trigger_appropriate_dag',
    python_callable=trigger_appropriate_dag,
    provide_context=True,
    dag=dag
)

trigger_monitoring = TriggerDagRunOperator(
    task_id="trigger_monitor_dags",
    trigger_dag_id="monitor_dags",  
    conf={"dag_name": dag.dag_id}, 
    dag=dag,
)

consume_from_topic >> determine_dag >> trigger_dag >> trigger_monitoring