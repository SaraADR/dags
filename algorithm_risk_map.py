import os
import json
from datetime import datetime, timedelta  # Importación correcta para manejar fechas
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.bash import BashOperator
from dag_utils import execute_query
import time

def execute_docker_process(**context):
    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

    try:
        with ssh_hook.get_conn() as ssh_client:
            print("Conectando por SSH y ejecutando Docker Compose...")

            command = "cd /home/admin3/algoritmo-mapas-de-riesgo && docker-compose up --build"
            stdin, stdout, stderr = ssh_client.exec_command(command)

            # Leer la salida de Docker en tiempo real
            for line in iter(stdout.readline, ""):
                print(f"{line.strip()}")  # Imprime cada línea en tiempo real para Airflow

            for line in iter(stderr.readline, ""):
                print(f"{line.strip()}")  # Captura errores en tiempo real también

            # Esperar hasta que el contenedor finalice
            check_command = "docker ps -q --filter 'name=mapa_riesgo'"
            while True:
                stdin, stdout, stderr = ssh_client.exec_command(check_command)
                running_containers = stdout.read().decode().strip()
                if not running_containers:
                    print("El contenedor ha finalizado.")
                    break
                print("Contenedor en ejecución... esperando 10 segundos más.")
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



def store_in_db(**context):
    import json
    from datetime import datetime

    local_file = context['task_instance'].xcom_pull(task_ids='download_output_file', key='output_file')

    if not local_file:
        print("No se encontró el archivo a guardar en la base de datos.")
        return

    # Datos a guardar
    datos = {
        "sampled_feature": "mapa_riesgo",  
        "result_time": datetime.utcnow(),
        "phenomenon_time": datetime.utcnow(),
        "input_data": json.dumps({"source": local_file}),
        "output_data": json.dumps({"status": "FINISHED", "file_path": local_file})
    }

    query = f"""
        INSERT INTO algoritmos.algoritmo_risk_maps (
            sampled_feature, result_time, phenomenon_time, input_data, output_data
        ) VALUES (
            '{datos['sampled_feature']}',
            '{datos['result_time']}',
            '{datos['phenomenon_time']}',
            '{datos['input_data']}',
            '{datos['output_data']}'
        )
    """

    try:
        execute_query('biobd', query)
        print("Datos guardados correctamente en la base de datos.")
    except Exception as e:
        print(f"Error al guardar en la base de datos: {str(e)}")


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

store_in_db_task = PythonOperator(
    task_id='store_in_db',
    python_callable=store_in_db,
    provide_context=True,
    dag=dag,
)

publish_geoserver_task = PythonOperator(
    task_id='publish_to_geoserver',
    python_callable=publish_to_geoserver,
    provide_context=True,
    dag=dag,
)

execute_docker_task >> check_output_task >> store_in_db_task >> publish_geoserver_task
