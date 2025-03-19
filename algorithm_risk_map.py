import os
import json
import time  # Importación correcta para usar time.sleep()
from datetime import datetime, timedelta  # Importación correcta para manejar fechas

# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.bash import BashOperator


def execute_docker_process(**context):
    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")
    
    try:
        with ssh_hook.get_conn() as ssh_client:
            print("Conectando por SSH y ejecutando Docker Compose...")
            command = "cd /home/admin3/algoritmo-mapas-de-riesgo && docker-compose up --build -d"
            stdin, stdout, stderr = ssh_client.exec_command(command)
            print(stdout.read().decode())
            print(stderr.read().decode())

            # Esperar hasta que el contenedor finalice
            check_command = "docker ps -q --filter 'name=mapa_riesgo'"
            while True:
                stdin, stdout, stderr = ssh_client.exec_command(check_command)
                running_containers = stdout.read().decode().strip()
                if not running_containers:
                    print("El contenedor ha finalizado.")
                    break
                time.sleep(10)  # Esperar 10 segundos antes de volver a verificar

    except Exception as e:
        print(f"Error en la ejecución del algoritmo: {str(e)}")
        raise

def check_output_files(**context):
    """
    Verifica si los archivos TIFF se generaron correctamente en la carpeta de salida del contenedor.
    """
    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")
    
    try:
        with ssh_hook.get_conn() as ssh_client:
            print("Verificando archivos de salida en /app/output/...")
            # Se ejecuta el comando sin parámetros interactivos
            command = "ls -l ~/algoritmo-mapas-de-riesgo/output"
            stdin, stdout, stderr = ssh_client.exec_command(command)
            output_files = stdout.read().decode()
            print("Archivos encontrados en /app/output/:")
            print(output_files)
            
            if "mapariesgo" not in output_files:
                raise Exception("No se generaron archivos TIFF en la carpeta de salida.")
    except Exception as e:
        print(f"Error al verificar archivos de salida: {str(e)}")
        raise

def publish_to_geoserver(**context):
    """
    Publica los resultados en Geoserver.
    (Implementación pendiente: aquí se incluirá la lógica para subir el TIFF a Geoserver)
    """
    print("Publicando en Geoserver... (Implementación pendiente)")

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG(
    'algorithm_risk_map',
    default_args=default_args,
    description='DAG para ejecutar el algoritmo de mapas de riesgo 6 veces al día automáticamente',
    schedule_interval='0 8,10,12,14,16,18 * * *',
    catchup=False,
    max_active_runs=1
)

execute_docker_task = PythonOperator(
    task_id='execute_docker_process',
    python_callable=execute_docker_process,
    provide_context=True,
    dag=dag,
)

check_output_task = PythonOperator(
    task_id='check_output_files',
    python_callable=check_output_files,
    provide_context=True,
    dag=dag,
)

publish_geoserver_task = PythonOperator(
    task_id='publish_to_geoserver',
    python_callable=publish_to_geoserver,
    provide_context=True,
    dag=dag,
)

execute_docker_task >> check_output_task >> publish_geoserver_task
