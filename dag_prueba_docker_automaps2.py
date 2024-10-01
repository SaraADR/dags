import datetime
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
    # Crear un directorio temporal
    temp_dir = '/tmp'
    os.makedirs(temp_dir, exist_ok=True)
    
    os.chdir(temp_dir)
    
    print("Comienza el dag")

    try:
        # Obtener conexión MinIO desde Airflow
        connection = BaseHook.get_connection('minio_conn')
        extra = json.loads(connection.extra)
        s3_client = boto3.client(
            's3',
            endpoint_url=extra['endpoint_url'],
            aws_access_key_id=extra['aws_access_key_id'],
            aws_secret_access_key=extra['aws_secret_access_key'],
            config=Config(signature_version='s3v4')
        )

        bucket_name = 'algorithms'

        # Define the objects and their local paths
        files_to_download = {
            'share_data/input/config.json': os.path.join(temp_dir, 'share_data/input/config.json'),
            'launch/.env': os.path.join(temp_dir, 'launch/.env'),
            'launch/automaps.tar': os.path.join(temp_dir, 'launch/automaps.tar'),
            'launch/compose.yaml': os.path.join(temp_dir, 'launch/compose.yaml'),
            'launch/run.sh': os.path.join(temp_dir, 'launch/run.sh')
        }

        # Create necessary directories
        for local_path in files_to_download.values():
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

        output_dir = os.path.join(temp_dir, 'share_data/output')
        os.makedirs(output_dir, exist_ok=True)


        # Descargar los archivos de MinIO
        for s3_key, local_path in files_to_download.items():
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            s3_client.download_file(bucket_name, s3_key, local_path)
            print(f"Descargado {s3_key} a {local_path}")


        print_directory_contents(temp_dir)
        ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')


        remote_file_path = '/proyectos/Autopymaps/launch/.env'
    
        # Ruta local donde deseas guardar el archivo
        local_file_path = '/tmp/launch/.env'
        
        # Crear directorios locales si no existen
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)


        ls = SSHOperator(
                task_id="ls",
                command= "ls -l",
                ssh_hook = ssh_hook,
                dag = dag)
        
        print(ls)

        pwd = SSHOperator(
                task_id="pwd",
                command= "pwd",
                ssh_hook = ssh_hook,
                dag = dag)
        
        print(pwd)

        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            try:
                # Verificar si el archivo remoto existe
                sftp.stat(local_file_path)  # Esto levantará una excepción si no existe
                # Descargar el archivo del servidor remoto
                sftp.get('/home/admin3/Automapsdok/ta.json', '/tmp/ta.json')
                print(f"Archivo {local_file_path} descargado exitosamente a {remote_file_path}")
            except FileNotFoundError:
                print(f"El archivo remoto {local_file_path} no se encontró.")
            except Exception as e:
                print(f"Error al descargar el archivo: {str(e)}")
            finally:
                # Cerrar la conexión SFTP
                sftp.close()
        print(f'Directorio temporal creado en: {temp_dir}')

        # rundocker(temp_dir)
        return temp_dir

    except Exception as e:
        print(f"Error: {str(e)}")
        return

    finally:
        # Limpieza del directorio temporal si es necesario
        pass

def print_directory_contents(directory):
    print(f"Contenido del directorio: {directory}")
    for root, dirs, files in os.walk(directory):
        level = root.replace(directory, '').count(os.sep)
        indent = ' ' * 4 * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print(f"{subindent}{f}")
    print("------------------------------------------")



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

