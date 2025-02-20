import datetime
import json
import os
from sqlalchemy import text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

from dag_utils import get_db_session

def execute_docker_process(**context):
    """ Ejecuta el proceso GIF utilizando el archivo JSON recibido desde otro DAG. """

    # Extraer datos directamente del DAG que hace trigger
    conf = context.get("dag_run").conf
    if not conf:
        print("Error: No se recibió configuración desde el DAG.")
        return
    
    print("Datos recibidos del DAG:")
    print(json.dumps(conf, indent=4))

    # Extraer información del JSON
    event_name = conf.get("eventName", "UnknownEvent")
    data_str = conf.get("data", {})

    if isinstance(data_str, str):
        try:
            data = json.loads(data_str)
        except json.JSONDecodeError:
            print("Error al decodificar 'data'")
            return
    else:
        data = data_str

    print(f"Evento: {event_name}")
    print(f"Datos extraídos antes de ajuste: {json.dumps(data, indent=4)}")

    if not data:
        print("Advertencia: No hay datos válidos para procesar.")
        return

    # Asegurar que `data` siempre sea una lista antes de subirlo
    if isinstance(data, dict):  
        data = [data]  

    print(f"Datos ajustados para Docker: {json.dumps(data, indent=4)}")

    # Subir el JSON al servidor para que lo use Docker
    remote_file_path = "/home/admin3/grandes-incendios-forestales/share_data_host/inputs/input_automatic.json"
    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

    try:
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()

            # Guardar JSON en el servidor
            with sftp.file(remote_file_path, "w") as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)

            print(f"Archivo JSON guardado correctamente en {remote_file_path}")

            # Ejecutar Docker Compose
            print("Ejecutando Docker Compose...")
            stdin, stdout, stderr = ssh_client.exec_command(
                "cd /home/admin3/grandes-incendios-forestales && docker-compose up -d"
            )
            output = stdout.read().decode()
            error_output = stderr.read().decode()
            print(f"Salida de la ejecución: {output}")
            print(f"Errores de la ejecución: {error_output}")

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

def obtener_id_mision(fire_id):
    """
    Obtiene el mission_id (idMision) a partir del fire_id desde la tabla mss_mission_fire.
    """
    try:
        session = get_db_session()  # Obtiene la sesión de SQLAlchemy
        
        query = text("""
            SELECT mission_id 
            FROM missions.mss_mission_fire 
            WHERE fire_id = :fire_id;
        """)
        
        result = session.execute(query, {'fire_id': fire_id}).fetchone()

        if result:
            return result[0]  # Devuelve el mission_id encontrado
        else:
            print(f"No se encontró mission_id para fire_id: {fire_id}")
            return None

    except Exception as e:
        print(f"Error al obtener mission_id: {e}")
        return None


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
