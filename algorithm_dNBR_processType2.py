from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.hooks.base_hook import BaseHook
import json
import pytz
from airflow.models import Variable
from dag_utils import execute_query
from sqlalchemy import text
from datetime import datetime, timedelta
import calendar
import requests


class FechaProxima:
    def __init__(self):
        self.hoy = datetime.today()

    def restar_meses(self, fecha, meses):
        mes = fecha.month - 1 - meses
        año = fecha.year + mes // 12
        mes = mes % 12 + 1
        dia = min(fecha.day, calendar.monthrange(año, mes)[1])
        return fecha.replace(year=año, month=mes, day=dia)
    
    def obtener_fechas_exactas(self, meses_minimo, meses_maximo):
        fechas = []
        for meses in range(int(meses_minimo), int(meses_maximo) + 1, int(meses_minimo)):
            fecha_resta = self.restar_meses(self.hoy, meses)
            if fecha_resta.day == self.hoy.day:
                fechas.append(fecha_resta.strftime("%Y-%m-%d"))
        return fechas


def process_element(**context):

    madrid_tz = pytz.timezone('Europe/Madrid')
    fechaHoraActual = datetime.now(madrid_tz)  # Fecha y hora con zona horaria

    print(f"Este algoritmo se está ejecutando a las {fechaHoraActual.strftime('%Y-%m-%d %H:%M:%S')} en Madrid, España")

    tipo2mesesminimo = Variable.get("dNBR_mesesFinIncendioMinimo", default_var="3")
    print(f"Valor de la variable tipo2mesesminimo en Airflow: {tipo2mesesminimo}")

    tipo2mesesmaximo = Variable.get("dNBR_mesesFinIncendioMaximo", default_var="1200")
    print(f"Valor de la variable tipo2mesesmaximo en Airflow: {tipo2mesesmaximo}")




    # Obtener fechas usando la clase FechaProxima
    fechas = FechaProxima()
    fechas_a_buscar = fechas.obtener_fechas_exactas(tipo2mesesminimo, tipo2mesesmaximo)
    print(f"Fechas calculadas: {fechas_a_buscar}")
    fechas_query = "','".join(fechas_a_buscar)

    query = f"""
        SELECT mf.fire_id
        FROM missions.mss_mission m
        JOIN missions.mss_mission_fire mf ON m.id = mf.mission_id
        WHERE mf.extinguishing_timestamp::DATE IN ('{fechas_query}')
    """

    result = execute_query('biobd', query)
    print(result)
    for record in result:
        print(record)
        ejecutar_algoritmo(record, fechaHoraActual)





def ejecutar_algoritmo(datos, fechaHoraActual):
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')
    fecha = fechaHoraActual.strftime("%d%m%Y")

    try:
        # Conectarse al servidor SSH
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            print(f"Sftp abierto")

            print(f"Cambiando al directorio de lanzamiento y ejecutando limpieza de volumenes")
            stdin, stdout, stderr = ssh_client.exec_command('cd /home/admin3/algoritmo_dNBR/launch && docker-compose down --volumes')
            stdout.channel.recv_exit_status()  # Esperar a que el comando termine

            for dato in datos:
                print(f"dato: {dato}")
                json_Incendio = busqueda_datos_incendio(dato)
                json_Perimetro = busqueda_datos_perimetro(dato)

                print(json_Incendio)
                print(json_Perimetro)

                idFire = dato

                if json_Incendio is not None:
                    archivo_incendio = f"/home/admin3/algoritmo_dNBR/input/ob_incendio/incendio_{idFire}_{fecha}.json"
                    ssh_client.exec_command(f"touch {archivo_incendio}")
                    ssh_client.exec_command(f"chmod 644 {archivo_incendio}")
                    with sftp.file(archivo_incendio, 'w') as json_file:
                        json.dump(json_Incendio, json_file, ensure_ascii=False, indent=4)

                if json_Perimetro is not None:                                    
                    archivo_perimetro = f"/home/admin3/algoritmo_dNBR/input/perimetros/perimetro_{idFire}_{fecha}.json"
                    ssh_client.exec_command(f"touch {archivo_perimetro}")
                    ssh_client.exec_command(f"chmod 644 {archivo_perimetro}")
                    with sftp.file(archivo_perimetro, 'w') as json_file:
                        json_file.write(json.dumps(json_Perimetro, indent=4).encode('utf-8'))
                              

                params = {
                    "directorio_alg":  '.',
                    "directorio_output" : '/share_data/output/' + str(idFire) + "_" + str(fecha),
                    "obj_incendio":  f'/share_data/input/ob_incendio/incendio_{idFire}_{fecha}.json',
                    "obj_perimetro":  f'/share_data/input/perimetros/perimetro_{idFire}_{fecha}.json',
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
                    with sftp.file(archivo_params, 'w') as json_file:
                        json.dump(params, json_file, ensure_ascii=False, indent=4)
                        print(f"Guardado archivo {archivo_params}")

                    path = f'/share_data/input/ejecucion_{idFire}_{fecha}.json' 
                    stdin, stdout, stderr = ssh_client.exec_command(f'cd /home/admin3/algoritmo_dNBR/scripts && CONFIGURATION_PATH={path} docker-compose -f ../launch/compose.yaml up --build')              
                    output = stdout.read().decode()
                    error_output = stderr.read().decode()

                    print("Salida de run.sh:")
                    print(output)
                    print(error_output)

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
    'start_date': datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'algorithm_dNBR_process_Type2',
    default_args=default_args,
    description='Algoritmo dNBR Type2',
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
