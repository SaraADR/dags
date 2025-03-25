from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth
import os

# Parámetros GeoServer
GEOSERVER_URL = "https://geoserver.swarm-training.biodiversidad.einforex.net/geoserver/rest"
GEOSERVER_USER = "admin"
GEOSERVER_PASSWORD = "geoserver"
WORKSPACE = "Modelos_Combustible_2024"
GENERIC_LAYER = "galicia_mapa_riesgo_latest"

def publish_riskmap_to_geoserver(**context):
    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")
    remote_output_dir = "/home/admin3/algoritmo_mapas_de_riesgo/output"

    with ssh_hook.get_conn() as ssh_client:
        sftp = ssh_client.open_sftp()
        sftp.chdir(remote_output_dir)

        # Buscar primer archivo que cumpla con "mapariesgo*.tif"
        candidates = [f for f in sftp.listdir() if f.startswith("mapariesgo") and f.endswith(".tif")]
        if not candidates:
            raise FileNotFoundError("No se encontraron archivos TIFF en el directorio remoto.")

        tiff_file_name = sorted(candidates)[-1]  # O usa [0] si prefieres el más antiguo
        remote_tiff_path = os.path.join(remote_output_dir, tiff_file_name)
        print(f"📂 Archivo seleccionado: {remote_tiff_path}")

        # Leer contenido del archivo
        with sftp.file(remote_tiff_path, 'rb') as remote_file:
            file_data = remote_file.read()
        sftp.close()

    # Nombre de capa en GeoServer
    layer_name = f"galicia_mapa_riesgo_{datetime.now().strftime('%Y%m%d_%H%M')}"
    headers = {"Content-type": "image/tiff"}

    # 1️⃣ Subir como nueva capa
    url_new = f"{GEOSERVER_URL}/workspaces/{WORKSPACE}/coveragestores/{layer_name}/file.geotiff"
    response = requests.put(
        url_new,
        headers=headers,
        data=file_data,
        auth=HTTPBasicAuth(GEOSERVER_USER, GEOSERVER_PASSWORD),
        params={"configure": "all"}
    )
    if response.status_code not in [201, 202]:
        raise Exception(f"Error al publicar nueva capa {layer_name}: {response.text}")
    print(f"✅ Capa publicada: {layer_name}")

    # 2️⃣ Actualizar capa genérica
    url_latest = f"{GEOSERVER_URL}/workspaces/{WORKSPACE}/coveragestores/{GENERIC_LAYER}/file.geotiff"
    response_latest = requests.put(
        url_latest,
        headers=headers,
        data=file_data,
        auth=HTTPBasicAuth(GEOSERVER_USER, GEOSERVER_PASSWORD),
        params={"configure": "all"}
    )
    if response_latest.status_code not in [201, 202]:
        raise Exception(f"Error al actualizar capa genérica: {response_latest.text}")
    print(f"✅ Capa genérica actualizada: {GENERIC_LAYER}")

# DAG Definition
with DAG(
    dag_id="publish_riskmap_to_geoserver",
    default_args={"owner": "avincis"},
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["geoserver", "riskmap", "galicia"]
) as dag:

    publish_task = PythonOperator(
        task_id="publish_riskmap_layer",
        python_callable=publish_riskmap_to_geoserver,
        provide_context=True
    )
