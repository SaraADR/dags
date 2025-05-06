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

    conf = context.get("dag_run").conf
    if not conf:
        print("Error: No se recibió configuración desde el DAG.")
        return
    
    print("Datos recibidos del DAG:")
    print(json.dumps(conf, indent=4))

    event_name = conf.get("eventName", "UnknownEvent")
    data_str = conf.get("data", {})

    
    if isinstance(data_str, str):
        try:
            data = json.loads(data_str)  # Convertir de string JSON a estructura Python
        except json.JSONDecodeError:
            print("Error al decodificar 'data'")
            return
    else:
        data = data_str

    
    if isinstance(data, dict):
        data = [data]  
    elif not isinstance(data, list):
        print("Error: 'data' no es una lista ni un diccionario válido")
        return

    print(f"Evento: {event_name}")
    print(f"Datos ajustados para Docker: {json.dumps(data, indent=4)}")

    if not data:
        print("Advertencia: No hay datos válidos para procesar.")
        return

    remote_file_path = "/home/admin3/grandes-incendios-forestales/share_data_host/inputs/input_automatic.json"
    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

    try:
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()

            with sftp.file(remote_file_path, "w") as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)

            print(f"Archivo JSON guardado correctamente en {remote_file_path}")

            print("Ejecutando Docker Compose...")
            stdin, stdout, stderr = ssh_client.exec_command(
                "cd /home/admin3/grandes-incendios-forestales && docker-compose up -d"
            )
            output = stdout.read().decode()
            error_output = stderr.read().decode()
            print(f"Salida de la ejecución: {output}")
            print(f"Errores de la ejecución: {error_output}")

            remote_output_path = "/home/admin3/grandes-incendios-forestales/share_data_host/expected/output.json"
            local_output_path = "/tmp/output.json"

            try:
                sftp.get(remote_output_path, local_output_path)
                print("Archivo de salida descargado correctamente.")
            except FileNotFoundError:
                print("output.json no encontrado. Continuando con la ejecución.")

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
        session = get_db_session()
        
        query = text("""
            SELECT mission_id 
            FROM missions.mss_mission_fire 
            WHERE fire_id = :fire_id;
        """)
        
        result = session.execute(query, {'fire_id': fire_id}).fetchone()

        if result:
            return result[0]
        else:
            print(f"No se encontró mission_id para fire_id: {fire_id}")
            return None

    except Exception as e:
        print(f"Error al obtener mission_id: {e}")
        return None

def obtener_mission_id_task(**context):
    """Accede al servidor vía SSH, descarga output.json, y obtiene mission_id utilizando fire_id."""
    remote_output_path = "/home/admin3/grandes-incendios-forestales/share_data_host/expected/output.json"
    local_output_path = "/tmp/output.json"
    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

    try:
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            sftp.get(remote_output_path, local_output_path)
            print(f"Archivo descargado correctamente: {local_output_path}")
            sftp.close()

        with open(local_output_path, "r") as file:
            resultado_json = json.load(file)

        fire_id = resultado_json[0]["id"]
        mission_id = obtener_id_mision(fire_id)

        if mission_id:
            print(f"Mission ID obtenido correctamente: {mission_id}")
            context['task_instance'].xcom_push(key='mission_id', value=mission_id)
            context['task_instance'].xcom_push(key='fire_id', value=fire_id)  
        else:
            print(f"No se encontró mission_id para fire_id: {fire_id}")

    except FileNotFoundError:
        print("output.json no encontrado en el servidor, no se puede obtener mission_id.")
    except Exception as e:
        print(f"Error en la tarea de obtener mission_id: {str(e)}")
        raise


def guardar_resultados_task(**context):
    """
    Obtiene mission_id y fire_id desde XCom, descarga output.json desde el servidor, y guarda los datos en la BD.
    """
    mission_id = context['task_instance'].xcom_pull(task_ids='obtener_mission_id', key='mission_id')
    fire_id = context['task_instance'].xcom_pull(task_ids='obtener_mission_id', key='fire_id')  # 🔁 Añadido
    input_data = context['dag_run'].conf.get("data")  

    if not mission_id:
        print("No se pudo obtener mission_id, no se guardarán los datos en la BD.")
        return

    if not input_data:
        print("No se recibió el JSON de entrada desde el DAG, no se guardará en la BD.")
        return

    if isinstance(input_data, str):
        try:
            input_data = json.loads(input_data)
        except json.JSONDecodeError:
            print("Error al decodificar 'input_data'")
            return

    if isinstance(input_data, dict):
        input_data = [input_data]
    elif not isinstance(input_data, list):
        print("Error: 'input_data' no es una lista ni un diccionario válido")
        return

    remote_output_path = "/home/admin3/grandes-incendios-forestales/share_data_host/expected/output.json"
    local_output_path = "/tmp/output.json"
    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")  

    try:
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            sftp.get(remote_output_path, local_output_path)
            sftp.close()

        with open(local_output_path, "r") as file:
            output_data_json = json.load(file)

        
        output_data = {
            "estado": "FINISHED",
            "type": 1,
            "fire_id": fire_id,
            "data": output_data_json
        }

    except FileNotFoundError:
        print("output.json no encontrado, registrando estado 'ERROR'.")
        output_data = {
            "estado": "ERROR",
            "comentario": "output.json no encontrado",
            "type": 1,
            "fire_id": fire_id
        }
    except Exception as e:
        print(f"Error en la tarea de guardar resultados: {str(e)}")
        output_data = {
            "estado": "ERROR",
            "comentario": str(e),
            "type": 1,
            "fire_id": fire_id
        }

    try:
        session = get_db_session()
        fecha_hoy = datetime.datetime.now(datetime.timezone.utc)

        datos = {
            'sampled_feature': mission_id,
            'result_time': fecha_hoy,
            'phenomenon_time': fecha_hoy,
            'input_data': json.dumps({"fire_id": fire_id}),  
            'output_data': json.dumps(output_data)
        }

        query = text("""
            INSERT INTO algoritmos.algoritmo_gifs_fire_prediction (
                sampled_feature, result_time, phenomenon_time, input_data, output_data
            ) VALUES (
                :sampled_feature, :result_time, :phenomenon_time, :input_data, :output_data
            ) RETURNING fid;
        """)

        result = session.execute(query, datos)
        session.commit()
        fid = result.fetchone()[0]
        context['task_instance'].xcom_push(key='fid', value=fid)

        print(f"Datos insertados correctamente en algoritmo_gifs_fire_prediction con fid {fid}")

    except Exception as e:
        session.rollback()
        print(f"Error en la tarea de guardar resultados en BD: {str(e)}")
        raise



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
# Proceso de ejecución de Docker
execute_docker_task = PythonOperator(
    task_id='execute_docker_process',
    python_callable=execute_docker_process,
    provide_context=True,
    dag=dag,
)

# Obtención del mission_id desde la base de datos
obtener_mission_id = PythonOperator(
    task_id='obtener_mission_id',
    python_callable=obtener_mission_id_task,
    provide_context=True,
    dag=dag,
)

# Guardado de los resultados en la base de datos
guardar_resultados = PythonOperator(
    task_id='guardar_resultados',
    python_callable=guardar_resultados_task,
    provide_context=True,
    dag=dag,
)

execute_docker_task >> obtener_mission_id >> guardar_resultados
