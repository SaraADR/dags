from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.hooks.base import BaseHook
import boto3
from botocore.client import Config
import base64
import json
import os
from airflow.providers.ssh.hooks.ssh import SSHHook



# Función para manejar la conexión a MinIO y subir archivos
def save_to_minio(unique_id):
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)

    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

    bucket_name = 'locationtest'
    file_name = f'image_{unique_id}.tiff'  # Usamos unique_id para el nombre del archivo

    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except s3_client.exceptions.NoSuchBucket:
        s3_client.create_bucket(Bucket=bucket_name)

    # Suponiendo que el contenido del archivo se obtiene de alguna manera
    file_content = b''  # Placeholder para el contenido del archivo

    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=file_content,
        Tagging=f"unique_id={unique_id}"
    )
    print(f'{file_name} subido correctamente a MinIO.')



# SSH Connection function to execute commands on a remote server
def ssh_connection():
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')
    try:
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            print("SSH abierto")
            # Execute the Docker command via SSH
            stdin, stdout, stderr = ssh_client.exec_command(
                'cd /home/admin3/exiftool/exiftool && docker run -v /home/admin3/exiftool/exiftool:/images --name exiftool-container-new exiftool-image -config /images/example1.1.0_missionId.txt -u /images/img-20231205115059007-vis.tiff '
            )

            

            # Attempt decoding output with UTF-8, fallback to latin-1 if necessary
            try:
                output = stdout.read().decode('utf-8')

                stdin, stdout, stderr = ssh_client.exec_command(
                'docker rm exiftool-container-new'
            )
            except UnicodeDecodeError:
                output = stdout.read().decode('latin-1')

            try:
                error_output = stderr.read().decode('utf-8')
            except UnicodeDecodeError:
                error_output = stderr.read().decode('latin-1')

            print("Salida de docker volumes:")
            print(output)
            if error_output:
                print("Error output:")
                print(error_output)

    except Exception as e:
        print(f"Error in SSH connection: {str(e)}")


# Define the actions based on the last digit of Gimbal Tilt
def handle_gimbal_tilt(gimbal_tilt):
    last_digit = gimbal_tilt[-1]  # Get the last character
    unique_id = "unique_value"  # Placeholder para un identificador único

    if last_digit == '1':
        print("Gimbal Tilt ends with 1: Creating resource and uploading to MinIO.")
        save_to_minio(unique_id)
    elif last_digit == '5':
        print("Gimbal Tilt ends with 5: Waiting, not uploading.")
    elif last_digit == '9':
        print("Gimbal Tilt ends with 9: Closing operation, not uploading.")
    else:
        print(f"Gimbal Tilt ends with {last_digit}: No specific action defined, not uploading.")

# Define a function to parse metadata
def parse_metadata(metadata):
    data = {}
    for line in metadata.splitlines():
        if ' : ' in line:
            key, value = line.split(' : ', 1)
            data[key.strip()] = value.strip()
    return data

# Function to process metadata
def process_metadata(**kwargs):
    ti = kwargs['ti']
    metadata = ti.xcom_pull(task_ids='run_docker')
    print(f"Metadata received: {metadata}")

    decoded_bytes = base64.b64decode(metadata)
    decoded_str = decoded_bytes.decode('utf-8')
    metadata_dict = parse_metadata(decoded_str)

    print(f"Metadata received:\n{json.dumps(metadata_dict, indent=4)}")

    gimbal_tilt = metadata_dict.get("Gimbal Tilt")

    if gimbal_tilt:
        print(f"Gimbal Tilt: {gimbal_tilt}")
        handle_gimbal_tilt(gimbal_tilt)
    else:
        print("Gimbal Tilt not found in metadata.")

# Define default arguments for the DAG
default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'tiff_control',
    default_args=default_args,
    description='DAG que controla los tiff llegados a Airflow',
    schedule_interval=None,
    catchup=False
)

# Define the tasks
print_message_task = PythonOperator(
    task_id='print_message',
    python_callable=lambda **context: print("El mensaje se ha recibido correctamente"),
    provide_context=True,
    dag=dag,
)

# run_docker_task = SSHOperator(
#     task_id='run_docker',
#     ssh_conn_id='ssh_docker',
#     command='docker run --rm -v /servicios/exiftool:/images --name exiftool-container-new exiftool-image -config /images/example2.0.0.txt -u /images/img-20231205115059007-vis.tiff',
#     dag=dag,
#     do_xcom_push=True,
# )

process_metadata_task = PythonOperator(
    task_id='process_metadata',
    python_callable=process_metadata,
    provide_context=True,
    dag=dag,
)

ssh_connection_task = PythonOperator(
    task_id='ssh_connection_task',
    python_callable=ssh_connection,
    provide_context=True,
    dag=dag,
)


# Define task dependencies
print_message_task >> ssh_connection_task >> process_metadata_task
