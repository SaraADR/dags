import datetime
import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

def execute_docker_process(**context):
    """ Ejecuta el proceso GIF utilizando el archivo JSON recibido desde Kafka/NiFi. """
    
    conf = context.get("dag_run").conf
    remote_file_path = "/home/admin3/grandes-incendios-forestales/input_automatic.json"
    local_file_path = "/tmp/input_automatic.json"
    
    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

    try:
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()

            # Descargar el JSON desde el servidor
            try:
                sftp.get(remote_file_path, local_file_path)
                print(f"Archivo JSON descargado correctamente desde {remote_file_path}")
            except FileNotFoundError:
                print(f"El archivo {remote_file_path} no existe en el servidor.")
                return

            # Leer y procesar el JSON
            with open(local_file_path, "r", encoding="utf-8") as json_file:
                input_data = json.load(json_file)

            print("Datos cargados desde el archivo JSON:")
            print(json.dumps(input_data, indent=4))

            # Extraer datos necesarios
            event_name = input_data.get('eventName', "UnknownEvent")
            data_str = input_data.get('data', {})

            if isinstance(data_str, str):
                try:
                    data = json.loads(data_str)
                except json.JSONDecodeError:
                    print("Error al decodificar 'data'")
                    return
            else:
                data = data_str

            print(f"Evento: {event_name}")
            print(f"Datos extraídos: {json.dumps(data, indent=4)}")

            # Guardar el JSON procesado en el servidor
            with sftp.file(remote_file_path, "w") as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)

            print(f"Archivo procesado guardado en {remote_file_path}")

            # Ejecutar Docker Compose
            print("Ejecutando Docker Compose...")
            ssh_client.exec_command(
                "cd /home/admin3/grandes-incendios-forestales && docker-compose up -d"
            )

            # Descargar output.json si existe
            remote_output_path = "/home/admin3/grandes-incendios-forestales/share_data_host/expected/output.json"
            local_output_path = "/tmp/output.json"

            try:
                sftp.get(remote_output_path, local_output_path)
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
