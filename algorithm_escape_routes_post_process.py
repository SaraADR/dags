from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from sqlalchemy import create_engine, Table, MetaData
from airflow.hooks.base import BaseHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.hooks.http_hook import HttpHook
import requests
from dag_utils import update_job_status, throw_job_error
import os




def process_escape_routes_data(**context):
    # Obtener los datos del contexto del DAG
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str)


    url = "https://actions-api.avincis.cuatrodigital.com/geo-files-locator/get-files-paths"  

    payload = [
        {
            "fileType": "hojasmtn50",
            "date": "2024-01-01",
            "location": "Galicia"
        },
        {
            "fileType": "combustible",
            "date": "2024-02-01",
            "location": "Galicia"
        },
        {
            "fileType": "vias",
            "date": "2024-02-01",
            "location": "Galicia"
        }
    ]

    headers = {
        "Content-Type": "application/json"
    }

    try:
        # Hacer la petición POST al endpoint
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        response.raise_for_status()  

        response_data = response.json()
        print("Datos obtenidos del endpoint de Hasura:")
        print(json.dumps(response_data, indent=4))

    except requests.exceptions.RequestException as e:
        print(f"Error al llamar al endpoint de Hasura: {e}")
        error_message = str(e)
        print(f"Error durante el guardado de la misión: {error_message}")
        # Actualizar el estado del job a ERROR y registrar el error
        # Obtener job_id desde el contexto del DAG
        job_id = context['dag_run'].conf['message']['id']        
        throw_job_error(job_id, e)
        raise




    # Procesar "inicio" y "destino" para permitir diferentes estructuras
    inicio = input_data.get('inicio', None)
    destino = input_data.get('destino', None)

    # Ajustar "inicio" y "destino" según los datos recibidos
    if isinstance(inicio, str):
        try:
            inicio = json.loads(inicio)
        except json.JSONDecodeError:
            inicio = None

    if isinstance(destino, list):
        try:
            destino = [json.loads(d) if isinstance(d, str) else d for d in destino]
        except json.JSONDecodeError:
            destino = None
    elif isinstance(destino, dict):
        pass

  

    # Crear un diccionario para almacenar los paths
    file_paths = {
        "dir_hojasmtn50": None,
        "dir_combustible": None,
        "dir_vias": None,
        "dir_cursos_agua": None,
        "dir_aguas_estancadas": None,
        "dir_carr_csv": None,
        "zonas_abiertas": None
    }

    # Rellenar el diccionario con los paths obtenidos del response
    for file in response_data:
        if "fileType" in file and "path" in file:
            if file["fileType"] == "hojasmtn50":
                file_paths["dir_hojasmtn50"] = file["path"]
            elif file["fileType"] == "combustible":
                file_paths["dir_combustible"] = file["path"]
            elif file["fileType"] == "vias":
                file_paths["dir_vias"] = file["path"]
            elif file["fileType"] == "cursos_agua":
                file_paths["dir_cursos_agua"] = file["path"]
            elif file["fileType"] == "aguas_estancadas":
                file_paths["dir_aguas_estancadas"] = file["path"]
            elif file["fileType"] == "carr_csv":
                file_paths["dir_carr_csv"] = file["path"]
            elif file["fileType"] == "zonas_abiertas":
                file_paths["zonas_abiertas"] = file["path"]

    params = {
        "dir_incendio": input_data.get('dir_incendio', None),
        "dir_mdt": input_data.get('dir_mdt', None),
        "dir_hojasmtn50": file_paths["dir_hojasmtn50"],
        "dir_combustible": file_paths["dir_combustible"],
        "api_idee": input_data.get('api_idee', True),
        "dir_vias": file_paths["dir_vias"],
        "dir_cursos_agua": file_paths["dir_cursos_agua"],
        "dir_aguas_estancadas": file_paths["dir_aguas_estancadas"],
        "inicio": inicio, 
        "destino": destino,  
        "direccion_avance": input_data.get('direccion_avance', None),
        "distancia": input_data.get('distancia', None),
        "dist_seguridad": input_data.get('dist_seguridad', None),
        "dir_obstaculos": input_data.get('dir_obstaculos', None),
        "dir_carr_csv": file_paths["dir_carr_csv"],
        "dir_output": '/share_data/output/' + 'rutas_escape_' + str(message['message']['id']),
        "sugerir": input_data.get('sugerir', False),
        "zonas_abiertas": file_paths["zonas_abiertas"],
        "v_viento": input_data.get('v_viento', None),
        "f_buffer": input_data.get('f_buffer', 100),
        "c_prop": input_data.get('c_prop', "Extremas"),
        "lim_pendiente": input_data.get('lim_pendiente', None),
        "dist_estudio": input_data.get('dist_estudio', 5000),
    }


    # Crear el JSON dinámicamente
    json_data = create_json(params)

    # Mostrar el JSON por pantalla
    print("JSON generado:")
    print(json.dumps(json_data, indent=4))


    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')
    try:
        # Conectarse al servidor SSH
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            print(f"Sftp abierto")

            id_ruta = str(message['message']['id'])
            carpeta_destino = f"/home/admin3/algoritmo-rutas-de-escape-algoritmo-2-master/input/input_{id_ruta}_rutas_escape"
            
            print(f"Creando carpeta y guardando el json en su interior: {carpeta_destino}")
            ssh_client.exec_command(f"mkdir -p {carpeta_destino}")
            ssh_client.exec_command(f"chmod 755 {carpeta_destino}")
            json_file_path = f"{carpeta_destino}/input_data_{id_ruta}.json"

            ssh_client.exec_command(f"touch -p {json_file_path}")
            ssh_client.exec_command(f"chmod 644 {json_file_path}")

            with sftp.file(json_file_path, 'w') as json_file:
                json.dumps(json_data, json_file, indent=4)
            
            print(f"Archivo JSON guardado en: {json_file_path}")

            command = f'cd /home/admin3/algoritmo-rutas-de-escape-algoritmo-2-master/launch &&  CONFIGURATION_PATH={json_file_path} docker-compose -f compose.yaml up --build'
            stdin, stdout, stderr = ssh_client.exec_command(command)
            output = stdout.read().decode()
            error_output = stderr.read().decode()
            exit_status = stdout.channel.recv_exit_status() 
            if exit_status != 0:
                print("Errores al ejecutar run.sh:")
                print(error_output)

            print("Salida de docker:")
            print(output)

            output_directory = '../output/' + 'rutas_escape_' + str(message['message']['id'])
            local_output_directory = '/tmp'

            sftp.chdir(output_directory)
            print(f"Cambiando al directorio de salida: {output_directory}")

            downloaded_files = []
            for filename in sftp.listdir():
                remote_file_path = os.path.join(output_directory, filename)
                local_file_path = os.path.join(local_output_directory, filename)

                # Descargar cada archivo
                sftp.get(remote_file_path, local_file_path)
                print(f"Archivo {filename} descargado a {local_file_path}")
                downloaded_files.append(local_file_path)
            sftp.close()

            if not downloaded_files:
                print("Errores al ejecutar run.sh:")
                print(error_output)
            
            else:
                print_directory_contents(local_output_directory)


    except Exception as e:
        print(f"Error en el proceso: {str(e)}")
        error_message = str(e)
        print(f"Error durante el guardado de la misión: {error_message}")
        # Actualizar el estado del job a ERROR y registrar el error
        # Obtener job_id desde el contexto del DAG
        job_id = context['dag_run'].conf['message']['id']        
        throw_job_error(job_id, e)
        raise


    finally:
        # Cerrar SFTP si está abierto
        if 'sftp' in locals():
            sftp.close()
            print("SFTP cerrado.")

    # Actualizar el estado del job a 'FINISHED' si todo se completa correctamente
    try:
        job_id = message['message']['id']
        update_job_status(job_id, "FINISHED")

    except Exception as e:
        error_message = str(e)
        print(f"Error durante el guardado de la misión: {error_message}")
        # Actualizar el estado del job a ERROR y registrar el error
        # Obtener job_id desde el contexto del DAG
        job_id = context['dag_run'].conf['message']['id']        
        throw_job_error(job_id, e)
        raise
    
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

def create_json(params):
    # Generar un JSON basado en los parámetros proporcionados
    input_data = {
        "dir_incendio": params.get("dir_incendio", None),
        "dir_mdt": params.get("dir_mdt", None),
        "dir_hojasmtn50": params.get("dir_hojasmtn50", None),
        "dir_combustible": params.get("dir_combustible", None),
        "api_idee": params.get("api_idee", True),
        "dir_vias": params.get("dir_vias", None),
        "dir_cursos_agua": params.get("dir_cursos_agua", None),
        "dir_aguas_estancadas": params.get("dir_aguas_estancadas", None),
        "inicio": params.get("inicio", None),
        "destino": params.get("destino", None),
        "direccion_avance": params.get("direccion_avance", None),
        "distancia": params.get("distancia", None),
        "dist_seguridad": params.get("dist_seguridad", None),
        "dir_obstaculos": params.get("dir_obstaculos", None),
        "dir_carr_csv": params.get("dir_carr_csv", None),
        "dir_output": params.get("dir_output", None),
        "sugerir": params.get("sugerir", False),
        "zonas_abiertas": params.get("zonas_abiertas", None),
        "v_viento": params.get("v_viento", None),
        "f_buffer": params.get("f_buffer", 100),
        "c_prop": params.get("c_prop", "Extremas"),
        "lim_pendiente": params.get("lim_pendiente", None),
        "dist_estudio": params.get("dist_estudio", 5000),
    }
    return input_data


# Configuración del DAG
default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'algorithm_escape_routes_post_process',
    default_args=default_args,
    description='DAG para generar JSON, mostrarlo por pantalla y actualizar estado del job',
    schedule_interval=None,
    catchup=False,
    concurrency=1
)

# Tarea para generar y mostrar el JSON
process_escape_routes_task = PythonOperator(
    task_id='process_escape_routes',
    provide_context=True,
    python_callable=process_escape_routes_data,
    dag=dag,
)

# Definir el flujo de tareas
process_escape_routes_task
