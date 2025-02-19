import datetime
import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

def execute_docker_process(**context):
    """ Ejecuta el proceso GIF utilizando el archivo JSON recibido desde Kafka/NiFi. """
    
    # Obtener la ruta del archivo JSON recibido desde el DAG que disparó esta ejecución
    conf = context.get("dag_run").conf
    file_path = conf.get("file_path", "/home/admin3/grandes-incendios-forestales/input_automatic.json")

    if not os.path.exists(file_path):
        print(f"El archivo {file_path} no existe.")
        return

    # Leer el archivo JSON
    with open(file_path, "r", encoding="utf-8") as json_file:
        input_data = json.load(json_file)

    print("Datos cargados desde el archivo JSON:")
    print(json.dumps(input_data, indent=4))

    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

    try:
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()

            # Subir `input_automatic.json` al servidor para Docker
            with sftp.file(file_path, "w") as json_file:
                json.dump(input_data, json_file, ensure_ascii=False, indent=4)

            print(f"Archivo de entrada guardado correctamente: {file_path}")

            # Ejecutar Docker Compose
            print("Ejecutando Docker Compose...")
            ssh_client.exec_command(
                "cd /home/admin3/grandes-incendios-forestales && docker-compose up -d"
            )

            # Intentar descargar output.json si existe
            output_path = "/home/admin3/grandes-incendios-forestales/share_data_host/expected/output.json"
            local_output_path = "/tmp/output.json"

            try:
                sftp.get(output_path, local_output_path)
                print("Archivo de salida descargado correctamente.")
            except FileNotFoundError:
                print("output.json no encontrado. Continuando con la ejecución.")

            # Limpiar contenedor después de la ejecución
            print("Eliminando contenedor...")
            ssh_client.exec_command(
                "cd /home/admin3/grandes-incendios-forestales && docker-compose down"
            )
            print("Contenedor eliminado correctamente.")

            sftp.close()

    except Exception as e:
        print(f"Error en la ejecución: {str(e)}")
        raise

# Configuración del DAG en Airflow
default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 8, 8),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

dag = DAG(
    'algorithm_gifs_fire_prediction_post_process',
    default_args=default_args,
    description='DAG que ejecuta GIFS Fire Prediction con Docker Compose',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

execute_docker_task = PythonOperator(
    task_id='execute_docker_process',
    python_callable=execute_docker_process,
    provide_context=True,
    dag=dag,
)

execute_docker_task
