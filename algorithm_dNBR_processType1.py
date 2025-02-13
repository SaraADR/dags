from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.hooks.base_hook import BaseHook
import json
import pytz
from airflow.models import Variable
from dag_utils import execute_query
from sqlalchemy import text
import requests

def process_element(**context):

    madrid_tz = pytz.timezone('Europe/Madrid')
    fechaHoraActual = datetime.datetime.now(madrid_tz)  # Fecha y hora con zona horaria

    print(f"Este algoritmo se está ejecutando a las {fechaHoraActual.strftime('%Y-%m-%d %H:%M:%S')} en Madrid, España")

    tipo1diasincendio = Variable.get("dNBR_diasFinIncendio", default_var="10")
    print(f"Valor de la variable tipo1diasincendio en Airflow: {tipo1diasincendio}")

    interval_value = f'{tipo1diasincendio} days'
    query = f"""
        SELECT mf.fire_id 
        FROM missions.mss_mission m
        JOIN missions.mss_mission_fire mf ON m.id = mf.mission_id
        WHERE mf.extinguishing_timestamp::DATE = (CURRENT_DATE - INTERVAL '{interval_value}')
    """

    result = execute_query('biobd', query)
    print(result)
    for record in result:
        print(record)
        ejecutar_algoritmo(record, fechaHoraActual)



def ejecutar_algoritmo(datos, fechaHoraActual):
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')
    fecha = datetime.strptime(fechaHoraActual, "%d %m %Y %H:%M:%S").strftime("%d%m%Y")

    try:
        # Conectarse al servidor SSH
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            print(f"Sftp abierto")

            print(f"Cambiando al directorio de lanzamiento y ejecutando limpieza de voluemnes")
            stdin, stdout, stderr = ssh_client.exec_command('cd /home/admin3/algoritmo_dNBR/launch && docker-compose down --volumes')
            stdout.channel.recv_exit_status()  # Esperar a que el comando termine

            for dato in datos:
                print(f"dato: {dato}")
                json_Incendio = busqueda_datos_incendio(dato)
                json_Perimetro = busqueda_datos_perimetro(dato)

                print(json_Incendio)
                print(json_Perimetro)

                idFire = dato.get('fireId')

                if json_Incendio is not None:
                    archivo_incendio = f"/home/admin3/algoritmo_dNBR/input/ob_incendio/incendio_{idFire}_{fecha}.json"
                    with open(archivo_incendio , 'w') as file:
                        json.dump(json_Incendio, file)
                        print(f"Guardado archivo {archivo_incendio }")

                if json_Perimetro is not None:                                    
                    archivo_perimetro = f"/home/admin3/algoritmo_dNBR/input/perimetros/perimetro_{idFire}_{fecha}.json"
                    with open(archivo_perimetro, 'w') as file:
                        json.dump(json_Perimetro, file)
                        print(f"Guardado archivo {archivo_perimetro}")
                              

                params = {
                    "directorio_alg":  '.',
                    "directorio_output" : '/share_data/output/' + idFire + "_" + fecha,
                    "obj_incendio":  archivo_incendio,
                    "obj_perimetro":  archivo_perimetro,
                    "service_account" : Variable.get("dNBR_path_serviceAccount", default_var=None), 
                    "credenciales" : '/share_data/input/algoritmos-bio-b40e24394020.json',
                    "dias_pre" :  Variable.get("dNBR_diasPre", default_var="10"),
                    "dias_post" : Variable.get("dNBR_diasPost", default_var="10"),
                    "pdefect" : None,
                    "dia_fin" :  None,
                    "buffer" : None ,
                    "combustibles" : Variable.get("dNBR_pathCombustible", default_var="/share_data/input/galicia_mod_com_filt.tif") 
                }
                print(params)

                if params is not None:                                    
                    archivo_params = f"/home/admin3/algoritmo_dNBR/input/ejecucion_{idFire}_{fecha}.json"
                    with open(archivo_params, 'w') as file:
                        json.dump(params, file)
                        print(f"Guardado archivo {archivo_params}")


                    stdin, stdout, stderr = ssh_client.exec_command(f'cd /home/admin3/algoritmo_dNBR/launch && CONFIGURATION_PATH={archivo_params} docker-compose -f compose.yaml up --build')              
                    output = stdout.read().decode()
                    error_output = stderr.read().decode()

                    print("Salida de run.sh:")
                    print(output)


            sftp.close()
    except Exception as e:
        print(f"Error en el proceso: {str(e)}")    
        raise        

    return None




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
                return fire_data
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
    'algorithm_dNBR_process_Type1',
    default_args=default_args,
    description='Algoritmo dNBR Type 1',
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