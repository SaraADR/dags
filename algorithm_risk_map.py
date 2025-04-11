import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
import pytz
from dag_utils import execute_query, get_geoserver_connection
import time
import requests
from requests.auth import HTTPBasicAuth
import re
from airflow.utils import timezone
import pendulum

def execute_docker_process(**context):
    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

    try:
        with ssh_hook.get_conn() as ssh_client:
            print("Conectando por SSH para limpiar y lanzar el contenedor...")

            # 1. Limpiar contenedores anteriores
            print("[INFO] Ejecutando docker-compose down...")
            cleanup_command = "cd /home/admin3/algoritmo_mapas_de_riesgo && docker-compose down --volumes"
            stdin, stdout, stderr = ssh_client.exec_command(cleanup_command)
            stdout.channel.recv_exit_status()  # Esperar que termine
            print("[OUTPUT][docker-compose down]:")
            for line in stdout.read().decode().splitlines():
                print(line)
            error_output = stderr.read().decode().strip()
            if error_output:
                print("[ERROR][docker-compose down]:")
                print(error_output)

            # 2. Lanzar el contenedor desde cero con build
            command = "cd /home/admin3/algoritmo_mapas_de_riesgo && docker-compose up --build -d"
            stdin, stdout, stderr = ssh_client.exec_command(command)

            stdout.channel.recv_exit_status()

            output = stdout.read().decode().strip()
            error_output = stderr.read().decode().strip()

            print("[OUTPUT][docker-compose up]:")
            for line in output.split("\n"):
                print(line)

            if error_output:
                print("[ERROR][docker-compose up]:")
                print(error_output)

            if "error" in error_output.lower():
                raise Exception("Fallo en la ejecución del contenedor. Revisar logs.")

            context['task_instance'].xcom_push(key='process_info', value={
                "execution_time": datetime.utcnow().isoformat(),
                "docker_output": output,
                "docker_errors": error_output,
                "status": "SUCCESS" if not error_output else "FAILED"
            })

            # 3. Esperar hasta que el contenedor finalice su ejecución
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

            # Extraer nombres de archivos .tif
            command_tif = "ls /home/admin3/algoritmo_mapas_de_riesgo/output/*.tif"
            stdin, stdout, stderr = ssh_client.exec_command(command_tif)
            tiff_files = stdout.read().decode().strip().split('\n')
            print("Archivos TIFF encontrados:", tiff_files)

            context['task_instance'].xcom_push(key='output_files', value=tiff_files)

            # Eliminar solo los TIFF que empiecen por 'mapariesgo' para evitar eliminar archivos no deseados
            delete_command = "find /home/admin3/algoritmo_mapas_de_riesgo/output -maxdepth 1 -type f -name 'mapariesgo*.tif' -delete"
            stdin, stdout, stderr = ssh_client.exec_command(delete_command)
            delete_output = stdout.read().decode().strip()
            delete_error = stderr.read().decode().strip()

            print("Resultado de eliminación de archivos TIFF:")
            print(delete_output)
            if delete_error:
                print("Errores al eliminar los archivos:")
                print(delete_error)



    except Exception as e:
        print(f"Error al verificar archivos de salida: {str(e)}")
        raise

def store_in_db(**context):
    process_info = context['task_instance'].xcom_pull(task_ids='execute_docker_process', key='process_info')
    tiff_files = context['task_instance'].xcom_pull(task_ids='check_output_files', key='output_files')

    madrid_tz = pytz.timezone('Europe/Madrid')

    if not process_info or not tiff_files:
        print("Falta información del proceso o archivos para guardar en la base de datos.")
        return

    # Tomamos el primer archivo .tif como ejemplo
    generated_file = os.path.basename(tiff_files[0]) if isinstance(tiff_files, list) else tiff_files
    generated_file_path = tiff_files[0] if isinstance(tiff_files, list) else tiff_files

    datos = {
        "sampled_feature": "mapa_riesgo",
        "result_time": datetime.now(madrid_tz),
        "phenomenon_time": datetime.now(madrid_tz),
        "input_data": json.dumps({
            "execution_time": process_info["execution_time"],
        }),
        "output_data": json.dumps({
            "generated_tiff": generated_file,
            "remote_path": generated_file_path,  
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



WORKSPACE = "Modelos_Combustible_2024"
GENERIC_LAYER = "galicia_mapa_riesgo_latest"
REMOTE_OUTPUT_DIR = "/home/admin3/algoritmo_mapas_de_riesgo/output"

def set_geoserver_style(layer_name, base_url, auth, style_name):
    url = f"{base_url}/layers/Modelos_Combustible_2024:{layer_name}"
    headers = {"Content-Type": "application/xml"}
    payload = f"""
        <layer>
            <defaultStyle>
                <name>{style_name}</name>
            </defaultStyle>
        </layer>
    """
    response = requests.put(url, headers=headers, data=payload, auth=auth)
    if response.status_code not in [200, 201]:
        raise Exception(f"Error asignando estilo a {layer_name}: {response.text}")
    print(f"Estilo '{style_name}' aplicado a capa '{layer_name}'")

def get_latest_file(ssh_client, file_list):
    sftp = ssh_client.open_sftp()
    latest = None
    latest_time = None
    for f in file_list:
        try:
            attr = sftp.stat(f)
            if latest_time is None or attr.st_mtime > latest_time:
                latest = f
                latest_time = attr.st_mtime
        except Exception as e:
            print(f"No se pudo acceder a {f}: {e}")
    sftp.close()
    return latest

def publish_to_geoserver(**context):
    WORKSPACE = "Modelos_Combustible_2024"
    GENERIC_LAYER = "galicia_mapa_riesgo_latest"

    tiff_files = context['task_instance'].xcom_pull(task_ids='check_output_files', key='output_files')
    if not tiff_files:
        raise Exception("No hay archivos para subir a GeoServer.")

    with SSHHook(ssh_conn_id="my_ssh_conn").get_conn() as ssh_client:
        latest_tiff = get_latest_file(ssh_client, tiff_files)

        if not latest_tiff:
            raise Exception("No se pudo determinar el archivo TIFF más reciente.")
        
        print(f"Publicando TIFF más reciente (por fecha real): {latest_tiff}")

        # Obtener nombre base del TIFF sin extensión
        base_name = os.path.splitext(os.path.basename(latest_tiff))[0]

        # Extraer fecha y hora ICONA del nombre del TIFF
        match = re.search(r'(\d{4})-(\d{2})-(\d{2})(\d{1,2})h', base_name)
        if match:
            yyyy, mm, dd, hh = match.groups()
            hh = hh.zfill(2)
            icona_timestamp = f"{yyyy}{mm}{dd}_{hh}h"
        else:
            raise Exception(f"No se pudo extraer fecha/hora ICONA del nombre: {base_name}")

        layer_name = f"galicia_mapa_riesgo_{icona_timestamp}"
        print(f"Nombre de capa a publicar: {layer_name}")

        # Leer archivo remoto
        sftp = ssh_client.open_sftp()
        with sftp.file(latest_tiff, 'rb') as remote_file:
            file_data = remote_file.read()
        sftp.close()

    # Subida a GeoServer
    base_url, auth = get_geoserver_connection("geoserver_connection")
    headers = {"Content-type": "image/tiff"}

    # Capa histórica
    url_new = f"{base_url}/workspaces/{WORKSPACE}/coveragestores/{layer_name}/file.geotiff"
    response = requests.put(url_new, headers=headers, data=file_data, auth=auth, params={"configure": "all"})
    if response.status_code not in [201, 202]:
        raise Exception(f"Error publicando {layer_name}: {response.text}")
    print(f"Capa publicada: {layer_name}")

    # Capa genérica
    url_latest = f"{base_url}/workspaces/{WORKSPACE}/coveragestores/{GENERIC_LAYER}/file.geotiff"
    response_latest = requests.put(url_latest, headers=headers, data=file_data, auth=auth, params={"configure": "all"})
    if response_latest.status_code not in [201, 202]:
        raise Exception(f"Error actualizando capa genérica: {response_latest.text}")
    print(f"Capa genérica actualizada: {GENERIC_LAYER}")

    # Estilo
    set_geoserver_style(layer_name, base_url, auth, "thermographic")
    set_geoserver_style("galicia_mapa_riesgo_latest", base_url, auth, "thermographic")


# Definición del DAG
default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 9, 1, tz="Europe/Madrid"),
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG(
    'algorithm_risk_map',
    default_args=default_args,
    description='DAG para ejecutar el algoritmo de mapas de riesgo 6 veces al día automáticamente',
    schedule_interval='0 8,10,12,14,16,18 * * *',
    catchup=False,
    max_active_runs=1,

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
