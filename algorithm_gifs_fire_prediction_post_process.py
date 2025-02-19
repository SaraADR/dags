import datetime
import json
import os
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

def execute_docker_process(**context):
    """ Ejecuta el proceso GIF en Modo Automático (A) dentro de Docker. """

    conf = context.get("dag_run").conf
    if not conf:
        print("Error: No se recibió configuración desde el DAG.")
        return
    
    print("Datos recibidos del DAG:")
    print(json.dumps(conf, indent=4))

    # Extraer y validar los datos
    event_name = conf.get("eventName", "UnknownEvent")
    data_str = conf.get("data", {})

    # Asegurar que `data` es un diccionario válido antes de guardarlo
    if isinstance(data_str, str):
        try:
            data = json.loads(data_str)
        except json.JSONDecodeError:
            print("Error al decodificar 'data'")
            return
    else:
        data = data_str

    if not data:
        print("No hay datos válidos para procesar.")
        return

    # Asegurar que `data` tiene la estructura correcta
    if isinstance(data, dict):  # Si es un solo incendio, convertirlo en lista
        data = [data]

    print(f"Datos extraídos y formateados: {json.dumps(data, indent=4)}")

    # Rutas en el contenedor
    remote_base_path = "/share_data"
    remote_input_path = f"{remote_base_path}/inputs/input_automatic.json"
    remote_output_path = f"{remote_base_path}/expected/output.json"
    
    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

    try:
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()

            # Crear la carpeta "inputs" si no existe
            try:
                sftp.mkdir(f"{remote_base_path}/inputs")
            except IOError:
                pass  # La carpeta ya existe

            # Subir JSON a la ruta correcta con la estructura exacta
            with sftp.file(remote_input_path, "w") as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)

            print(f"Archivo JSON guardado en {remote_input_path}")

            # Ejecutar Docker Compose (el script `main.py` se ejecuta automáticamente)
            print("Ejecutando el algoritmo en Docker...")
            stdin, stdout, stderr = ssh_client.exec_command(
                "cd /home/admin3/grandes-incendios-forestales && docker-compose down && docker-compose up -d"
            )
            print(stdout.read().decode())  # Ver la salida
            print(stderr.read().decode())  # Ver errores

            # Esperar unos segundos para que el modelo procese el JSON
            time.sleep(15)  # Esperar 15 segundos antes de descargar output.json

            # Descargar `output.json` si se generó correctamente
            local_output_path = "/tmp/output.json"

            try:
                sftp.get(remote_output_path, local_output_path)
                print("Archivo de salida descargado correctamente.")
            except FileNotFoundError:
                print("output.json no encontrado. Algo falló en la ejecución del algoritmo.")

            # Limpiar contenedor después de la ejecución
            print("Eliminando contenedor Docker...")
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
