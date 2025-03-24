import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
import pytz
from dag_utils import execute_query
import time

def execute_docker_process(**context):
    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

    try:
        with ssh_hook.get_conn() as ssh_client:
            print("Conectando por SSH y ejecutando Docker Compose...")
            command = "cd /home/admin3/algoritmo_mapas_de_riesgo && docker-compose up -d"
            stdin, stdout, stderr = ssh_client.exec_command(command)

            output = stdout.read().decode().strip()
            error_output = stderr.read().decode().strip()

            print("Salida del contenedor:")
            print(output)
            print("Errores del contenedor:")
            print(error_output)

            if "error" in error_output.lower():
                raise Exception("Fallo en la ejecución del contenedor. Revisar logs.")

            context['task_instance'].xcom_push(key='process_info', value={
                "execution_time": datetime.utcnow().isoformat(),
                "docker_output": output,
                "docker_errors": error_output,
                "status": "SUCCESS" if not error_output else "FAILED"
            })

            check_command = "docker ps -q --filter 'name=mapa_riesgo'"
            while True:
                stdin, stdout, stderr = ssh_client.exec_command(check_command)
                running_containers = stdout.read().decode().strip()
                if not running_containers:
                    print("El contenedor ha finalizado.")
                    break
                time.sleep(10)

    except Exception as e:
        print(f"Error en la ejecución del algoritmo: {str(e)}")
        raise

def check_output_files(**context):
    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

    try:
        with ssh_hook.get_conn() as ssh_client:
            print("Verificando archivos de salida en /app/output/...")
            command = "ls -l /home/admin3/algoritmo_mapas_de_riesgo/output"
            stdin, stdout, stderr = ssh_client.exec_command(command)

            output_files = stdout.read().decode().strip()
            print("Archivos encontrados:")
            print(output_files)

            if "mapariesgo" not in output_files:
                raise Exception("No se generaron archivos TIFF en la carpeta de salida.")

            context['task_instance'].xcom_push(key='output_files', value=output_files)

    except Exception as e:
        print(f"Error al verificar archivos de salida: {str(e)}")
        raise

def store_in_db(**context):
    process_info = context['task_instance'].xcom_pull(task_ids='execute_docker_process', key='process_info')
 # Y guardamos en la tabla de historico
    madrid_tz = pytz.timezone('Europe/Madrid')

    if not process_info:
        print("No se encontró información del proceso para guardar en la base de datos.")
        return

    datos = {
        "sampled_feature": "mapa_riesgo",
        "result_time": datetime.now(madrid_tz),
        "phenomenon_time": datetime.now(madrid_tz),
        "input_data": json.dumps({"execution_time": process_info["execution_time"]}),
        "output_data": json.dumps({
            "status": process_info["status"],
            "docker_output": process_info["docker_output"],
            "docker_errors": process_info["docker_errors"]
        })
    }

    query = """
        INSERT INTO algoritmos.algoritmo_risk_maps (
            sampled_feature, result_time, phenomenon_time, input_data, output_data
        ) VALUES (
            :sampled_feature, :result_time, :phenomenon_time, :input_data, :output_data
        )
    """

    try:
        execute_query('biobd', query, datos)
        print("Datos del proceso guardados correctamente en la base de datos.")
    except Exception as e:
        print(f"Error al guardar en la base de datos: {str(e)}")

def publish_to_geoserver(**context):
    print("Publicando en Geoserver...")

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
