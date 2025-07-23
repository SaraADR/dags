import ast
import base64
import io
import json
import os
import uuid
import zipfile
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
import tempfile
from confluent_kafka import Consumer, KafkaException
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import paramiko
from dag_utils import get_minio_client,download_from_minio
from airflow.hooks.base import BaseHook


KAFKA_RAW_MESSAGE_PREFIX = "Mensaje crudo:"
def poll_kafka_messages(**kwargs):
    conf = {
        'bootstrap.servers': '10.96.180.179:9092',
        'group.id': '1',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    consumer = Consumer(conf)
    consumer.subscribe(['intentoarchivosv2'])
    messages = []

    try:
        while True:
            msg = consumer.poll(timeout=10.0)
            if msg is None:
                print("No hay más mensajes que leer en el topic")
                break  
            if msg.error():
                raise KafkaException(msg.error())
            else:
                msg_value = msg.value().decode('utf-8')
                print("Mensaje procesado: ", msg_value)
                messages.append(msg_value)

        if messages:
                    print(f"Total messages received: {len(messages)}")
                    consumer.commit()

                    s3_client = get_minio_client()

                    # Nombre del bucket donde está almacenado el archivo/carpeta
                    bucket_name = 'tmp'
                    folder_prefix = 'sftp/'

                    local_directory = 'temp'  
                    for msg_value in messages:
                        print(f"{KAFKA_RAW_MESSAGE_PREFIX} {msg_value}")
                        file_path_in_minio =  msg_value
                        try:
                            local_zip_path = download_from_minio(s3_client, bucket_name, file_path_in_minio, local_directory, folder_prefix)
                            process_zip_file(local_zip_path, file_path_in_minio, msg_value,  **kwargs)
                            delete_file_sftp(msg_value)
                        except Exception as e:
                            print(f"Error al descargar desde MinIO: {e}")
                            raise 
    finally:
        consumer.close()





def delete_file_sftp(url):

    filename = os.path.basename(url)
    filename = "/upload/" + filename
    print(f"Filename para borrar: {filename}" )
    try:
        conn = BaseHook.get_connection('SFTP')
        host = conn.host
        port = conn.port 
        username = conn.login
        password = conn.password


        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        sftp.remove(filename)
        print(f"Archivo '{filename}' eliminado exitosamente.")

        # Cerrar conexiones
        sftp.close()
        transport.close()

    except Exception as e:
        print(f"Error al eliminar el archivo: {e}")



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
        algorithm_id = None

        with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
            if 'algorithm_result.json' in zip_file.namelist():
                with zip_file.open('algorithm_result.json') as f:
                    json_content = json.load(f)
                    json_content_metadata = json_content.get('metadata', [])
                    
                    # Buscar el campo 'AlgorithmID'
                    for metadata in json_content_metadata:
                        if metadata.get('name') == 'AlgorithmID':
                            algorithm_id = metadata.get('value')
                print(f"AlgorithmID encontrado: {algorithm_id}")

                if(algorithm_id == 'WaterAnalysis' or algorithm_id == 'MetashapeCartografia' or algorithm_id == 'FlameFront'):
                    print("Se ejecuta algoritmo de zip largo")
                    dag_names = {
                        'WaterAnalysis': 'water_analysis',
                        'MetashapeCartografia': 'algorithm_metashape',
                        'FlameFront': 'algorithm_flame_front'
                    }
                    trigger_dag_name = dag_names.get(algorithm_id)
                    unique_id = uuid.uuid4()


                    try:
                        trigger = TriggerDagRunOperator(
                            task_id=str(unique_id),
                            trigger_dag_id=trigger_dag_name,
                            conf={'json': json_content, 'otros': message},
                            execution_date=datetime.now().replace(tzinfo=timezone.utc),
                            dag=kwargs.get('dag'),
                        )
                        trigger.execute(context=kwargs)
                        return
                    except Exception as e:
                        print(f"Error al desencadenar el DAG: {e}")

            else:
                print("El archivo 'algorithm_result.json' no se encuentra en el ZIP.")
                unique_id = uuid.uuid4()
                trigger_dag_name = 'zips_no_algoritmos'

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
                return


            
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

                    unique_id = uuid.uuid4()


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
    except zipfile.BadZipFile as e:
        print(f"El archivo no es un ZIP válido: {e}")
        return




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


poll_task = PythonOperator(
        task_id='poll_kafka',
        python_callable=poll_kafka_messages,
        provide_context=True,
        dag=dag
)


poll_task 