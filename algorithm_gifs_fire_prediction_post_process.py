from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import requests
import geopandas as gpd
from shapely.geometry import Point
import os
from dag_utils import get_db_session  
from sqlalchemy import text
from airflow.providers.ssh.hooks.ssh import SSHHook

# Configuración de conexión SSH
ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

# Función auxiliar para ejecutar comandos en el servidor remoto vía SSH
def execute_remote_command(command):
    with ssh_hook.get_conn() as ssh_client:
        print(f"Ejecutando comando en SSH: {command}")
        stdin, stdout, stderr = ssh_client.exec_command(command)
        output, error = stdout.read().decode(), stderr.read().decode()
        if error:
            print(f"Error en SSH: {error}")
            raise Exception(error)
        return output

# Obtener datos meteorológicos desde Meteomatics API
def get_weather_data(**kwargs):
    ti = kwargs['ti']
    lat, lon = 42.56103, -8.618725

    with open("/home/admin3/grandes-incendios-forestales/project/conf.d/api_keys.json") as f:
        api_keys = json.load(f)
        meteomatics_user = api_keys["api_keys"]["meteomatics_user"]
        meteomatics_password = api_keys["api_keys"]["meteomatics_password"]

    parameters = [
        "tmax_2m_24h:C", "wind_speed_10m:ms", "relative_humidity_2m:p",
        "t_2m:C", "wind_dir_10m:d", "dew_point_2m:C", "tmin_2m_24h:C"
    ]
    url = f"https://api.meteomatics.com/{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}/{','.join(parameters)}/{lat},{lon}/json"

    response = requests.get(url, auth=(meteomatics_user, meteomatics_password))
    if response.status_code == 200:
        weather_data = response.json()
        ti.xcom_push(key='weather_data', value=weather_data)
    else:
        raise Exception(f"Error en Meteomatics: {response.text}")

# Determinar la zona fitoclimática desde un shapefile
def get_fitoclima(**kwargs):
    ti = kwargs['ti']
    lat, lon = 42.56103, -8.618725

    zonas_fitoclima = gpd.read_file("/home/admin3/grandes-incendios-forestales/data/zonas_fitoclima_galicia.shp")
    gdf_punto = gpd.GeoDataFrame(geometry=[Point(lon, lat)], crs="EPSG:4326")
    gdf_punto = gdf_punto.to_crs(zonas_fitoclima.crs)

    zona_fitoclimatica = next((zona["id"] for _, zona in zonas_fitoclima.iterrows() if gdf_punto.geometry.iloc[0].within(zona.geometry)), "Desconocido")

    ti.xcom_push(key='fitoclima', value=zona_fitoclimatica)

# Ejecutar la predicción en el servidor remoto vía SSH
def run_prediction(**kwargs):
    ti = kwargs['ti']

    weather_data = ti.xcom_pull(task_ids='get_weather_data', key='weather_data')
    fitoclima = ti.xcom_pull(task_ids='get_fitoclima', key='fitoclima')

    input_data = {
        "id": 0,
        "lat": 42.56103,
        "long": -8.618725,
        "zona_fitoclimatica": fitoclima,
        "tempmax_fecha_inicio": weather_data["data"][0]["value"],
        "windspeed_fecha_inicio": weather_data["data"][1]["value"],
        "humidity_fecha_inicio": weather_data["data"][2]["value"],
        "temp_fecha_inicio": weather_data["data"][3]["value"],
        "winddir_media_3dias": weather_data["data"][4]["value"],
        "dew_fecha_inicio": weather_data["data"][5]["value"],
        "tempmin_fecha_inicio": weather_data["data"][6]["value"]
    }

    remote_input_path = "/home/admin3/grandes-incendios-forestales/share_data/inputs/input_auto.json"
    ssh_hook.run(f"echo '{json.dumps([input_data])}' > {remote_input_path}")

    container_name = "gifs_service"
    remote_output_path = "/share_data/expected/output.json"
    command = f"docker exec {container_name} python app/src/algorithm_gifs_fire_prediction_post_process.py {remote_input_path} {remote_output_path} A"
    execute_remote_command(command)

# Guardar resultados en PostgreSQL
def save_results(**kwargs):
    ti = kwargs['ti']
    remote_output_path = "/home/admin3/grandes-incendios-forestales/share_data/expected/output.json"
    local_output_path = "/tmp/output.json"

    ssh_hook.get(remote_output_path, local_output_path)

    with open(local_output_path, "r") as f:
        output_data = json.load(f)

    session = get_db_session()
    engine = session.get_bind()

    try:
        for fire in output_data:
            query = text("""
                INSERT INTO predicciones_incendios (id_incendio, latitud, longitud, zona_fitoclimatica, prediccion)
                VALUES (:id, :lat, :long, :zona_fitoclimatica, :prediccion)
            """)
            session.execute(query, {
                "id": fire["id"],
                "lat": fire["lat"],
                "long": fire["long"],
                "zona_fitoclimatica": fire["zona_fitoclimatica"],
                "prediccion": fire["prediccion"]
            })

        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

# Configuración del DAG
default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'algorithm_gifs_fire_prediction_post_process',
    default_args=default_args,
    description='DAG para predicción de incendios en servidor remoto',
    schedule_interval=None,
    catchup=False
)

# Definir tareas en Airflow
get_weather_task = PythonOperator(task_id='get_weather_data', python_callable=get_weather_data, dag=dag)
get_fitoclima_task = PythonOperator(task_id='get_fitoclima', python_callable=get_fitoclima, dag=dag)
run_prediction_task = PythonOperator(task_id='run_prediction', python_callable=run_prediction, dag=dag)
save_results_task = PythonOperator(task_id='save_results', python_callable=save_results, dag=dag)

# Definir flujo de ejecución
get_weather_task >> get_fitoclima_task >> run_prediction_task >> save_results_task
