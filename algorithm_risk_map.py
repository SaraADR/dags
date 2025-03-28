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
import os


def execute_docker_process(**context):
    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

    try:
        with ssh_hook.get_conn() as ssh_client:
            print("Conectando por SSH para limpiar y lanzar el contenedor...")

            # 1. Detener y eliminar contenedores + volúmenes previos
            cleanup_command = "cd /home/admin3/algoritmo_mapas_de_riesgo && docker-compose down --volumes"
            stdin, stdout, stderr = ssh_client.exec_command(cleanup_command)
            cleanup_output = stdout.read().decode().strip()
            cleanup_error = stderr.read().decode().strip()
            print("Resultado de limpieza:")
            print(cleanup_output)
            if cleanup_error:
                print("Errores durante limpieza:")
                print(cleanup_error)

            # 2. Lanzar el contenedor desde cero con build
            command = "cd /home/admin3/algoritmo_mapas_de_riesgo && docker-compose up --build -d"
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

    datos = {
        "sampled_feature": "mapa_riesgo",
        "result_time": datetime.now(madrid_tz),
        "phenomenon_time": datetime.now(madrid_tz),
        "input_data": json.dumps({
            "execution_time": process_info["execution_time"],
        }),
        "output_data": json.dumps({
            "status": process_info["status"],
            "generated_tiff": generated_file,
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

def publish_to_geoserver(**context):
    import os
    import re
    import requests
    from datetime import datetime
    from airflow.providers.ssh.hooks.ssh import SSHHook
    from dag_utils import get_geoserver_connection

    WORKSPACE = "Modelos_Combustible_2024"
    GENERIC_LAYER = "galicia_mapa_riesgo_latest"

    # Obtener TIFF más reciente
    tiff_files = context['task_instance'].xcom_pull(task_ids='check_output_files', key='output_files')
    if not tiff_files:
        raise Exception("No hay archivos para subir a GeoServer.")

    latest_tiff = sorted(tiff_files)[-1]
    print(f"Publicando solo el TIFF más reciente: {latest_tiff}")

    # Obtener nombre base del TIFF sin extensión
    base_name = os.path.splitext(os.path.basename(latest_tiff))[0]

    # Extraer fecha y hora ICONA del nombre del TIFF
    match = re.search(r'(\d{4})-(\d{2})-(\d{2})(\d{1,2})h', base_name)
    if match:
        yyyy, mm, dd, hh = match.groups()
        hh = hh.zfill(2)  # Asegurar que la hora tenga dos dígitos
        icona_timestamp = f"{yyyy}{mm}{dd}_{hh}h"
    else:
        raise Exception(f"No se pudo extraer fecha/hora ICONA del nombre: {base_name}")

    # Construir nombre de capa final
    layer_name = f"galicia_mapa_riesgo_{icona_timestamp}"
    print(f"Nombre de capa a publicar: {layer_name}")

    # Leer archivo TIFF remoto vía SFTP
    with SSHHook(ssh_conn_id="my_ssh_conn").get_conn() as ssh_client:
        sftp = ssh_client.open_sftp()
        with sftp.file(latest_tiff, 'rb') as remote_file:
            file_data = remote_file.read()
        sftp.close()

    # Subida a GeoServer
    base_url, auth = get_geoserver_connection("geoserver_connection")
    headers = {"Content-type": "image/tiff"}

    # Capa histórica
    url_new = f"{base_url}/workspaces/{WORKSPACE}/coveragestores/{layer_name}/file.geotiff"
    response = requests.put(
        url_new,
        headers=headers,
        data=file_data,
        auth=auth,
        params={"configure": "all"}
    )
    if response.status_code not in [201, 202]:
        raise Exception(f"Error publicando {layer_name}: {response.text}")
    print(f"Capa publicada: {layer_name}")

    # Capa fija (latest)
    url_latest = f"{base_url}/workspaces/{WORKSPACE}/coveragestores/{GENERIC_LAYER}/file.geotiff"
    response_latest = requests.put(
        url_latest,
        headers=headers,
        data=file_data,
        auth=auth,
        params={"configure": "all"}
    )
    if response_latest.status_code not in [201, 202]:
        raise Exception(f"Error actualizando capa genérica: {response_latest.text}")
    print(f"Capa genérica actualizada: {GENERIC_LAYER}")




default_args = {
    'owner': 'oscar',
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
