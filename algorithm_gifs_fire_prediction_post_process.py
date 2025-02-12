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


# Función para obtener datos meteorológicos desde Meteomatics API
def get_weather_data(**context):
    ti = context['ti']
    lat, lon = 42.56103, -8.618725

    # Conexión SSH
    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")
    remote_path = "/home/admin3/grandes-incendios-forestales/project/conf.d/api_keys.json"
    local_path = "/tmp/api_keys.json"

    print("Descargando credenciales desde el servidor remoto...")
    with ssh_hook.get_conn() as ssh_client:
        sftp = ssh_client.open_sftp()
        sftp.get(remote_path, local_path)  # Descarga el archivo remoto a /tmp en Airflow
        sftp.close()

    # Leer las credenciales desde el archivo descargado
    with open(local_path, "r") as f:
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
        print("Datos meteorológicos obtenidos con éxito.")
    else:
        raise Exception(f"Error en Meteomatics: {response.text}")

def get_fitoclima(**context):
    ti = context['ti']
    lat, lon = 42.56103, -8.618725

    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

    # Ruta en el servidor remoto
    remote_dir = "/home/admin3/grandes-incendios-forestales/project/data/"
    shapefile_base = "zonas_fitoclima_galicia"
    local_dir = "/tmp/"

    print("Descargando todos los archivos del shapefile desde el servidor remoto...")
    with ssh_hook.get_conn() as ssh_client:
        sftp = ssh_client.open_sftp()

        # Descargar los archivos esenciales del shapefile (.shp, .dbf, .shx, .prj si existe)
        for ext in ["shp", "dbf", "shx", "prj"]:
            remote_file = f"{remote_dir}{shapefile_base}.{ext}"
            local_file = f"{local_dir}{shapefile_base}.{ext}"

            try:
                sftp.get(remote_file, local_file)
                print(f"Archivo descargado: {remote_file} -> {local_file}")
            except FileNotFoundError:
                print(f"Advertencia: No se encontró {remote_file}. Puede que el shapefile no tenga este archivo.")

        sftp.close()

    # Intentar abrir el shapefile con GeoPandas
    local_shapefile_path = f"{local_dir}{shapefile_base}.shp"
    print(f"Intentando abrir {local_shapefile_path} con GeoPandas...")
    
    try:
        zonas_fitoclima = gpd.read_file(local_shapefile_path)
    except Exception as e:
        raise Exception(f"Error al abrir el shapefile: {e}")

    # Crear un GeoDataFrame con el punto de coordenadas
    gdf_punto = gpd.GeoDataFrame(geometry=[Point(lon, lat)], crs="EPSG:4326")
    gdf_punto = gdf_punto.to_crs(zonas_fitoclima.crs)

    # Determinar en qué zona fitoclimática se encuentra el punto
    zona_fitoclimatica = next(
        (zona["id"] for _, zona in zonas_fitoclima.iterrows() if gdf_punto.geometry.iloc[0].within(zona.geometry)),
        "Desconocido"
    )

    ti.xcom_push(key='fitoclima', value=zona_fitoclimatica)
    print(f"Zona fitoclimática determinada: {zona_fitoclimatica}")



# Ejecutar la predicción en el servidor remoto usando SSH
def run_prediction(**context):
    ti = context['ti']
    
    # Obtener datos desde XCom
    weather_data = ti.xcom_pull(task_ids='get_weather_data', key='weather_data')
    fitoclima = ti.xcom_pull(task_ids='get_fitoclima', key='fitoclima')

    print("Datos meteorológicos recibidos:")
    print(json.dumps(weather_data, indent=4))  # 🔹 Mostrar estructura del JSON para depuración

    # Verificar que 'data' esté en weather_data
    if "data" not in weather_data:
        raise Exception("Error: 'data' no encontrado en la respuesta de Meteomatics. Verifica la API.")

    # Crear el input_data asegurando que todas las claves existen
    input_data = {
        "id": 0,
        "lat": 42.56103,
        "long": -8.618725,
        "zona_fitoclimatica": fitoclima
    }

    # Extraer los valores de los parámetros meteorológicos de forma segura
    parametros = [
        "tmax_2m_24h:C", "wind_speed_10m:ms", "relative_humidity_2m:p",
        "t_2m:C", "wind_dir_10m:d", "dew_point_2m:C", "tmin_2m_24h:C"
    ]

    for param in parametros:
        valor = None
        for item in weather_data["data"]:
            if item.get("parameter") == param:
                # Extraer el valor dentro de coordinates -> dates -> value
                try:
                    valor = item["coordinates"][0]["dates"][0]["value"]
                except (IndexError, KeyError):
                    print(f"Advertencia: No se encontró 'value' para el parámetro {param}.")
                break
        
        input_data[param] = valor

    print("Datos preparados para la predicción:")
    print(json.dumps(input_data, indent=4))

    # Conexión SSH para subir los datos al servidor remoto
    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")
    remote_input_path = "/home/admin3/grandes-incendios-forestales/share_data/inputs/input_auto.json"
    
    command_upload = f"echo '{json.dumps([input_data])}' > {remote_input_path}"

    print("Conectando al servidor remoto para subir los datos...")
    with ssh_hook.get_conn() as ssh_client:
        ssh_client.exec_command(command_upload)
    
    # Ejecutar la predicción en el servidor Docker
    container_name = "gifs_service"
    remote_output_path = "/share_data/expected/output.json"
    command_run = f"docker exec {container_name} python app/src/algorithm_gifs_fire_prediction_post_process.py {remote_input_path} {remote_output_path} A"

    print("Ejecutando la predicción en el servidor remoto...")
    with ssh_hook.get_conn() as ssh_client:
        stdin, stdout, stderr = ssh_client.exec_command(command_run)
        print(stdout.read().decode())
        error = stderr.read().decode()
        if error:
            print(f"Error en ejecución remota: {error}")
            raise Exception(error)


# Guardar resultados en PostgreSQL
def save_results(**context):
    ti = context['ti']
    remote_output_path = "/home/admin3/grandes-incendios-forestales/share_data/expected/output.json"
    local_output_path = "/tmp/output.json"

    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

    print("Descargando el archivo de resultados desde el servidor remoto...")
    with ssh_hook.get_conn() as ssh_client:
        sftp = ssh_client.open_sftp()
        sftp.get(remote_output_path, local_output_path)
        sftp.close()

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
        print("Resultados almacenados en la base de datos.")
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

# 🔹 Definir tareas en Airflow
get_weather_task = PythonOperator(task_id='get_weather_data', python_callable=get_weather_data, dag=dag)
get_fitoclima_task = PythonOperator(task_id='get_fitoclima', python_callable=get_fitoclima, dag=dag)
run_prediction_task = PythonOperator(task_id='run_prediction', python_callable=run_prediction, dag=dag)
save_results_task = PythonOperator(task_id='save_results', python_callable=save_results, dag=dag)

# 🔹 Definir flujo de ejecución
get_weather_task >> get_fitoclima_task >> run_prediction_task >> save_results_task
