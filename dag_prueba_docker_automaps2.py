import datetime
import io
import os
import shutil
import subprocess
from airflow import DAG
import tempfile
from airflow.hooks.base_hook import BaseHook
import json
import boto3
from botocore.client import Config, ClientError
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator

def find_the_folder():
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')

    try:
        # Conectarse al servidor SSH
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()

            print(f"Sftp abierto")

            remote_directory = '/home/admin3/Autopymaps/share_data/input'
            remote_file_name = 'config.json'
            remote_file_path = os.path.join(remote_directory, remote_file_name)

            sftp.chdir(remote_directory)
            print(f"Cambiando al directorio: {remote_directory}")

            with sftp.file(remote_file_path, 'r') as remote_file:
                file_data = remote_file.read()  # Leer el contenido del archivo
                print("Contenido del archivo original:")
                print(file_data)



            sftp.close()

            # Cambiar al directorio de lanzamiento y ejecutar run.sh
            print(f"Cambiando al directorio de lanzamiento y ejecutando run.sh")
            stdin, stdout, stderr = ssh_client.exec_command('cd /home/admin3/Autopymaps/launch && ./run.sh')
            
            output = stdout.read().decode()
            error_output = stderr.read().decode()

            print("Salida de run.sh:")
            print(output)
            
            if error_output:
                print("Errores al ejecutar run.sh:")
                print(error_output)

    except Exception as e:
        print(f"Error en el proceso: {str(e)}")








    # # Crear un directorio temporal
    # temp_dir = '/tmp'
    # os.makedirs(temp_dir, exist_ok=True)
    
    # os.chdir(temp_dir)
    
    # print("Comienza el dag")

    # try:
    #     # Obtener conexión MinIO desde Airflow
    #     connection = BaseHook.get_connection('minio_conn')
    #     extra = json.loads(connection.extra)
    #     s3_client = boto3.client(
    #         's3',
    #         endpoint_url=extra['endpoint_url'],
    #         aws_access_key_id=extra['aws_access_key_id'],
    #         aws_secret_access_key=extra['aws_secret_access_key'],
    #         config=Config(signature_version='s3v4')
    #     )

    #     bucket_name = 'algorithms'

    #     # Define the objects and their local paths
    #     files_to_transfer = {
    #         # 'share_data/input/config.json': '/Automapsdok/share_data/input/config.json',
    #         # 'launch/.env': '/Automapsdok/launch/.env',
    #         # 'launch/automaps.tar': '/Automapsdok/launch/automaps.tar',
    #         # 'launch/compose.yaml': '/Automapsdok/launch/compose.yaml',
    #         'launch/run.sh': '/Automapsdok/launch/run.sh'
    #     }

    #     ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')

    #     # Descargar el archivo de MinIO en memoria
    #     with ssh_hook.get_conn() as ssh_client:
    #         sftp = ssh_client.open_sftp()

    #         for minio_object_key, sftp_remote_path in files_to_transfer.items():
    #             try:
    #                 response = s3_client.get_object(Bucket=bucket_name, Key=minio_object_key)
    #                 file_data = response['Body'].read()  
    #                 print("file data:", file_data[-10:]) 

    #                 print("remote path")
    #                 print(sftp_remote_path)

    #                 print("remote path:", sftp_remote_path)
    #                 remote_directory = os.path.dirname(sftp_remote_path)
    #                 print("remote directory:", remote_directory)



    #                 # Subir el archivo al servidor SSH usando putfo
    #                 with io.BytesIO(file_data) as file_stream:
    #                     print("fileString")



    #                     stdin, stdout, stderr = ssh_client.exec_command('pwd')
    #                     # Leer la salida del comando
    #                     current_directory = stdout.read().decode().strip()
    #                     print(f"Directorio de trabajo actual: {current_directory}")


    #                     file_stream.seek(0)  # Asegúrate de que el puntero esté al principio
    #                     sftp.putfo(file_stream, sftp_remote_path)
    #                     print(f"Archivo {minio_object_key} transferido a {sftp_remote_path}")

      
    #             except Exception as e:
    #                 print(f"Error al transferir {minio_object_key}: {str(e)}")
    #         sftp.close()

    # except Exception as e:
    #     print(f"Error en el proceso: {str(e)}")






        # with ssh_hook.get_conn() as ssh_client:
        #     sftp = ssh_client.open_sftp()
        #     try:

        #         stdin, stdout, stderr = ssh_client.exec_command('pwd')
        #         # Leer la salida del comando
        #         current_directory = stdout.read().decode().strip()
        #         print(f"Directorio de trabajo actual: {current_directory}")
        #         error = stderr.read().decode().strip()
        #         if error:
        #             print(f"Error al ejecutar el comando: {error}")


        #         # Verificar si el archivo remoto existe
        #         sftp.stat(local_file_path)  # Esto levantará una excepción si no existe
        #         # Descargar el archivo del servidor remoto
        #         sftp.put('/Automapsdok/ta.json', '/tmp/launch/.env')
        #         print(f"Archivo {local_file_path} descargado exitosamente a {remote_file_path}")
        #     except FileNotFoundError:
        #         print(f"El archivo remoto {local_file_path} no se encontró.")
        #     except Exception as e:
        #         print(f"Error al descargar el archivo: {str(e)}")
        #     finally:
        #         # Cerrar la conexión SFTP
        #         sftp.close()


    #     print(f'Directorio temporal creado en: {temp_dir}')

    #     # rundocker(temp_dir)
    #     return temp_dir

    # except Exception as e:
    #     print(f"Error: {str(e)}")
    #     return

    # finally:
    #     # Limpieza del directorio temporal si es necesario
    #     pass

# def print_directory_contents(directory):
#     print(f"Contenido del directorio: {directory}")
#     for root, dirs, files in os.walk(directory):
#         level = root.replace(directory, '').count(os.sep)
#         indent = ' ' * 4 * level
#         print(f"{indent}{os.path.basename(root)}/")
#         subindent = ' ' * 4 * (level + 1)
#         for f in files:
#             print(f"{subindent}{f}")
#     print("------------------------------------------")



# def rundocker(temp_dir):
#     print("RUNDOCKER")

#     os.chdir(temp_dir)
#     print(temp_dir)



#     # Verifica si la imagen existe, si no, cárgala
#     image_name = "launch-automap_service:latest"
#     load_image_command = f"docker image load -i {temp_dir}/launch/automaps.tar"

#     try:
#         # Comando para verificar si la imagen ya existe
#         image_check_command = f"docker images -q {image_name}"
#         image_exists = subprocess.run(image_check_command, shell=True, stdout=subprocess.PIPE)

#         if not image_exists.stdout:  # Si no existe la imagen
#             print("La imagen no existe. Cargando imagen...")
#             try:
#                 result = subprocess.run(load_image_command, shell=True, check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
#                 print(result.stdout.decode())  # Muestra la salida estándar
#             except subprocess.CalledProcessError as e:
#                 print(f"Error al cargar la imagen: {e.stderr.decode()}")  # Muestra el error
#         else :
#             print("la imagen ya existe, la usamos")


#         print_directory_contents(temp_dir)


#         # Ahora ejecuta el contenedor usando docker-compose
#         container_name = os.getenv('CONTAINER_NAME', 'autopymaps_1') 
#         docker_compose_command = f"docker-compose -f {temp_dir}/launch/compose.yaml run --rm --name {container_name} automap_service"

#         try:
#             result = subprocess.run(docker_compose_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#             # Imprime la salida estándar
#             print("Salida estándar:")
#             print(result.stdout.decode())
#         except subprocess.CalledProcessError as e:
#             print(f"Error ejecutando docker-compose: {e.stderr.decode()}")


#         print("proceso finalizado")

#     except subprocess.CalledProcessError as e:
#         print(f"Error ejecutando el comando: {e}")


default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

dag = DAG(
    'dag_prueba_docker2',
    default_args=default_args,
    description='Algoritmo dag_prueba_docker',
    schedule_interval=None,
    catchup=False
)


#Cambia estado de job
find_the_folder_task = PythonOperator(
    task_id='ejecutar_run',
    python_callable=find_the_folder,
    dag=dag,
)

find_the_folder_task  

