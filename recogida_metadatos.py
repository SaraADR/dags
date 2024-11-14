from airflow import DAG
from datetime import datetime, timedelta, timezone
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.hooks.base_hook import BaseHook
import boto3
import json
from botocore.client import Config
import os
from botocore.exceptions import ClientError
from airflow.providers.ssh.hooks.ssh import SSHHook


def consumer_function(message, prefix, **kwargs):
    print(f"Mensaje crudo: {message}")
    try:
        msg_value = message.value().decode('utf-8')
        print("Mensaje procesado: ", msg_value)
    except Exception as e:
        print(f"Error al procesar el mensaje: {e}")

    
    file_path_in_minio =  msg_value  
        
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
    folder_prefix = 'metadatos/'

    # Descargar el archivo desde MinIO
    local_directory = 'temp'  # Cambia este path al local
    try:
        local_zip_path = download_from_minio(s3_client, bucket_name, file_path_in_minio, local_directory, folder_prefix)
        process_file(local_zip_path, file_path_in_minio, msg_value,  **kwargs)
    except Exception as e:
        print(f"Error al descargar desde MinIO: {e}")
        raise 


def download_from_minio(s3_client, bucket_name, file_path_in_minio, local_directory, folder_prefix):
    """
    Función para descargar archivos o carpetas desde MinIO.
    """
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    local_file = os.path.join(local_directory, os.path.basename(file_path_in_minio))
    print(f"Descargando archivo desde MinIO: {file_path_in_minio} a {local_file}")
    
    relative_path = file_path_in_minio.replace('/temp/', '')
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



def process_file(local_path, nombre_fichero, message, **kwargs):

    if local_path is None:
        print(f"No se pudo descargar el archivo desde MinIO: {local_path}")
        return
    
    print(local_path)
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')

    try:
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()

        
            if not nombre_fichero.endswith('/'):

                shared_volume_path = f"/home/admin3/exiftool/exiftool/images/{local_path}"

                sftp.put(local_path, shared_volume_path)
                print(f"Copied {local_path} to {shared_volume_path}")


                # # TODO: EL CONFIG NO TIENE POR QUE SER EL 2.0.0 HAY QUE MIRAR EL METADATO DE VERSION
                # docker_command = (
                #     f'cd /home/admin3/exiftool/exiftool && '
                #     f'docker run --rm -v /home/admin3/exiftool/exiftool:/images '
                #     f'--name exiftool-container-{file_name.replace(".", "-")} '
                #     f'exiftool-image -config /images/example2.0.0.txt -u /images/images/{file_name}'
                # )

                # stdin, stdout, stderr = ssh_client.exec_command(docker_command , get_pty=True)
                # output = ""
                # outputlimp = ""

                # for line in stdout:
                #     output += line.strip() + "\n"

                # print(f"Salida de docker command para {file_name}:")

                # # Clean up Docker container after each run
                # cleanup_command = f'docker rm exiftool-container-{file_name.replace(".", "-")}'
                # ssh_client.exec_command(cleanup_command)

    except Exception as e:
        print(f"Error in SSH connection: {str(e)}")


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
    'kafka_consumer_metadatos',
    default_args=default_args,
    description='DAG que consume mensajes de Kafka de imagenes y metadatos',
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=10,
)

consume_from_topic = ConsumeFromTopicOperator(
    kafka_config_id="kafka_connection",
    task_id="consume_from_topic_metadatos",
    topics=["metadatos"],
    apply_function=consumer_function,
    apply_function_kwargs={"prefix": "consumed:::"},
    commit_cadence="end_of_batch",
    dag=dag,
)


consume_from_topic