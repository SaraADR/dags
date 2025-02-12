import os
import json
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from shapely.geometry import Point
import geopandas as gpd
import subprocess
from dag_utils import get_db_session, throw_job_error, update_job_status
from sqlalchemy import text

# Ruta del archivo de configuración
CONF_PATH = "/home/admin3/grandes-incendios-forestales/project/conf.d/api_keys.json"

# Función para obtener datos climáticos desde Meteomatics API
def get_weather_data(**kwargs):
    ti = kwargs['ti']

    # Verificar si el archivo existe
    if not os.path.exists(CONF_PATH):
        raise FileNotFoundError(f"No se encontró el archivo de credenciales en {CONF_PATH}")

    # Cargar credenciales
    with open(CONF_PATH) as f:
        api_keys = json.load(f)
        meteomatics_user = api_keys["api_keys"]["meteomatics_user"]
        meteomatics_password = api_keys["api_keys"]["meteomatics_password"]

    # Coordenadas de la consulta
    lat, lon = 42.56103, -8.618725

    # Parámetros meteorológicos requeridos
    parameters = [
        "tmax_2m_24h:C", "wind_speed_10m:ms", "relative_humidity_2m:p",
        "t_2m:C", "wind_dir_10m:d", "dew_point_2m:C", "tmin_2m_24h:C"
    ]
    url = f"https://api.meteomatics.com/{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}/{','.join(parameters)}/{lat},{lon}/json"

    # Hacer la solicitud
    response = requests.get(url, auth=(meteomatics_user, meteomatics_password))
    if response.status_code == 200:
        weather_data = response.json()
        ti.xcom_push(key='weather_data', value=weather_data)
    else:
        raise Exception(f"Error en Meteomatics: {response.text}")

# Función para obtener la zona fitoclimática desde un shapefile
def get_fitoclima(**kwargs):
    ti = kwargs['ti']

    # Ruta del shapefile
    shapefile_path = "/home/admin3/grandes-incendios-forestales/project/data/zonas_fitoclima_galicia.shp"
    if not os.path.exists(shapefile_path):
        raise FileNotFoundError(f"Shapefile no encontrado en {shapefile_path}")

    # Cargar shapefile
    zonas_fitoclima = gpd.read_file(shapefile_path)
    gdf_punto = gpd.GeoDataFrame(geometry=[Point(-8.618725, 42.56103)], crs="EPSG:4326")

    # Reproyectar al CRS del shapefile
    gdf_punto = gdf_punto.to_crs(zonas_fitoclima.crs)

    # Buscar la zona fitoclimática
    zona_fitoclimatica = "Desconocido"
    for _, zona in zonas_fitoclima.iterrows():
        if gdf_punto.geometry.iloc[0].within(zona.geometry):
            zona_fitoclimatica = zona["id"]
            break

    ti.xcom_push(key='fitoclima', value=zona_fitoclimatica)

# Función para ejecutar la predicción en Docker
def run_prediction(**kwargs):
    ti = kwargs['ti']
    
    # Obtener datos de XCom
    weather_data = ti.xcom_pull(task_ids='get_weather_data', key='weather_data')
    fitoclima = ti.xcom_pull(task_ids='get_fitoclima', key='fitoclima')

    # Crear JSON de entrada
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

    # Guardar JSON
    input_file_path = "/home/admin3/grandes-incendios-forestales/project/share_data/inputs/input_auto.json"
    with open(input_file_path, "w") as f:
        json.dump([input_data], f, indent=4)

    # Ejecutar Docker
    container_name = "gifs_service"
    output_file = "/share_data/expected/output.json"

    command = f"docker exec {container_name} python app/src/algorithm_gifs_fire_prediction_post_process.py {input_file_path} {output_file} A"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        raise Exception(f"Error ejecutando la predicción: {result.stderr}")

# Guardar los resultados en la BD
def save_results(**kwargs):
    ti = kwargs['ti']
    output_file_path = "/home/admin3/grandes-incendios-forestales/project/share_data/expected/output.json"

    if not os.path.exists(output_file_path):
        raise FileNotFoundError(f"El archivo de salida no se encontró: {output_file_path}")

    with open(output_file_path, "r") as f:
        output_data = json.load(f)

    # Conectar a la base de datos
    session = get_db_session()

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
    description='DAG de predicción de incendios',
    schedule_interval=None,
    catchup=False
)

# Tareas
get_weather_task = PythonOperator(task_id='get_weather_data', python_callable=get_weather_data, dag=dag)
get_fitoclima_task = PythonOperator(task_id='get_fitoclima', python_callable=get_fitoclima, dag=dag)
run_prediction_task = PythonOperator(task_id='run_prediction', python_callable=run_prediction, dag=dag)
save_results_task = PythonOperator(task_id='save_results', python_callable=save_results, dag=dag)

get_weather_task >> get_fitoclima_task >> run_prediction_task >> save_results_task
