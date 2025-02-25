import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from dag_utils import execute_query
from airflow.hooks.base_hook import BaseHook
import requests


def process_element(**context):
    fire_id = 225305

    query = f"""
        SELECT mission_id
        FROM missions.mss_mission_fire mf
        WHERE mf.fire_id = {fire_id}
    """
    missionId = execute_query('biobd', query)

    json_Incendio = busqueda_datos_incendio(fire_id)
    json_Perimetro = busqueda_datos_perimetro(fire_id)

    print(missionId)
    print(json_Incendio)
    print(json_Perimetro)

    params = {
        "geometry":  json_Perimetro,
        "direction" : 250,
        "fireEvolutionVectorId": 1111,
        "limitTemperature" : 50,
        "minDistance" :30,
        "maxDistance" : 120,
        "firePerimeterId" : 225310   
    }



    try:
        ejecutar_algoritmo(params)
    except Exception as e:
        print(f"Error en la ejecución, el algoritmo ha dado un error en su salida")

    return


def ejecutar_algoritmo(params):
     return 0


def busqueda_datos_incendio(idIncendio):
        try:
            print("Buscando el incendio en einforex")
            # Conexión al servicio ATC usando las credenciales almacenadas en Airflow
            conn = BaseHook.get_connection('atc_services_connection')
            auth = (conn.login, conn.password)
            url = f"{conn.host}/rest/FireService/get?id={idIncendio}"

            response = requests.get(url, auth=auth)

            if response.status_code == 200:
                print("Incendio encontrado con exito.")
                fire_data = response.json()
                return fire_data
            else:
                print(f"Error en la busqueda del incendio: {response.status_code}")
                print(response.text)
                raise Exception(f"Error en la busqueda del incendio: {response.status_code}")

        except Exception as e:
            print(e)
            raise



def busqueda_datos_perimetro(idIncendio):
        try:
            print("Buscando el perimetro del incendio en einforex")
            # Conexión al servicio ATC usando las credenciales almacenadas en Airflow
            conn = BaseHook.get_connection('atc_services_connection')
            auth = (conn.login, conn.password)
            url = f"{conn.host}/rest/FireAlgorithm_FirePerimeterService/getByFire?id={idIncendio}"

            response = requests.get(url, auth=auth)

            if response.status_code == 200:
                print("Perimetros del incendio encontrados con exito.")
                fire_data = response.json()
                most_recent_obj = max(fire_data, key=lambda x: x["timestamp"])
                return most_recent_obj
            else:
                print(f"Error en la busqueda del incendio: {response.status_code}")
                print(response.text)
                raise Exception(f"Error en la busqueda del incendio: {response.status_code}")

        except Exception as e:
            print(e)
            raise


default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),

}

dag = DAG(
    'algorithm_work_zones',
    default_args=default_args,
    description='Algoritmo zonas de trabajo',
    schedule_interval='@daily', 
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

process_element_task = PythonOperator(
    task_id='process_message',
    python_callable=process_element,
    provide_context=True,
    dag=dag,
)

process_element_task 