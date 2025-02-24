from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.hooks.base_hook import BaseHook
import json
import pytz
from airflow.models import Variable
from sqlalchemy import text
import datetime
import calendar
import requests
from dag_utils import upload_to_minio_path, execute_query, print_directory_contents
import uuid
import os


class FechaProxima:
    def __init__(self):
        self.hoy = datetime.datetime.today()

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
    fechaHoraActual = datetime.datetime.now(madrid_tz)  # Fecha y hora con zona horaria

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
        SELECT mf.fire_id, m.id
        FROM missions.mss_mission m
        JOIN missions.mss_mission_fire mf ON m.id = mf.mission_id
        WHERE mf.extinguishing_timestamp::DATE IN ('{fechas_query}')
    """

    result = execute_query('biobd', query)
    print(result)
    for record in result:
        print(record)
        try:
            ejecutar_algoritmo(record, fechaHoraActual)
        except Exception as e:
            print(f"Error en la ejecución del algoritmo para {record}: el algoritmo ha dado un error en su salida")
            continue  # Continuar con la siguiente iteración







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

                fire_id, mission_id = datos

                if json_Incendio is not None:
                    archivo_incendio = f"/home/admin3/algoritmo_dNBR/input/ob_incendio/incendio_{fire_id}_{fecha}.json"
                    ssh_client.exec_command(f"touch {archivo_incendio}")
                    ssh_client.exec_command(f"chmod 644 {archivo_incendio}")
                    with sftp.file(archivo_incendio, 'w') as json_file:
                        json.dump(json_Incendio, json_file, ensure_ascii=False, indent=4)

                if json_Perimetro is not None:                                    
                    archivo_perimetro = f"/home/admin3/algoritmo_dNBR/input/perimetros/perimetro_{fire_id}_{fecha}.json"
                    ssh_client.exec_command(f"touch {archivo_perimetro}")
                    ssh_client.exec_command(f"chmod 644 {archivo_perimetro}")
                    with sftp.file(archivo_perimetro, 'w') as json_file:
                        json_file.write(json.dumps(json_Perimetro, indent=4).encode('utf-8'))
                              

                params = {
                    "directorio_alg":  '.',
                    "directorio_output" : '/share_data/output/' + str(fire_id) + "_" + str(fecha),
                    "obj_incendio":  f'/share_data/input/ob_incendio/incendio_{fire_id}_{fecha}.json',
                    "obj_perimetro":  f'/share_data/input/perimetros/perimetro_{fire_id}_{fecha}.json',
                    "service_account" : Variable.get("dNBR_path_serviceAccount", default_var=None), 
                    "credenciales" : '/share_data/input/algoritmos-bio-b40e24394020.json',
                    "dias_pre" :  int(Variable.get("dNBR_diasPre", default_var=10)),
                    "dias_post" : int(Variable.get("dNBR_diasPost", default_var=10)),
                    "dias_fin": (datetime.datetime.now() - datetime.timedelta(days=10)).strftime("%Y-%m-%d"),
                    "combustibles" : Variable.get("dNBR_pathCombustible", default_var="/share_data/input/galicia_mod_com_filt.tif") 
                }
                print(params)

                if params is not None:                                    
                    archivo_params = f"/home/admin3/algoritmo_dNBR/input/ejecucion_{fire_id}_{fecha}.json"
                    with sftp.file(archivo_params, 'w') as json_file:
                        json.dump(params, json_file, ensure_ascii=False, indent=4)
                        print(f"Guardado archivo {archivo_params}")

                    path = f'/share_data/input/ejecucion_{fire_id}_{fecha}.json' 
                    runId = f'{fire_id}_{fecha}'
                    stdin, stdout, stderr = ssh_client.exec_command(
                        f'cd /home/admin3/algoritmo_dNBR/scripts && '
                        f'export CONFIGURATION_PATH={path} && '
                        f'docker-compose -f ../launch/compose.yaml up --build && '
                        f'docker-compose -f ../launch/compose.yaml down --volumes'
                    )
                    output = stdout.read().decode()
                    error_output = stderr.read().decode()
                    output_data = {}
                    print("Salida de run.sh:")
                    print(output)
                    if "Valor -3" in output:
                        print("⚠️ Se encontró el mensaje de error: Valor -3")
                        sftp.close()
                        return
                    else:
                        # raise Exception(f"Error en la ejecución del script remoto: {error_output}")
                        output_directory = f'/home/admin3/algoritmo_dNBR/output/' + str(fire_id) + "_" + str(fecha) 
                        local_output_directory = f'/tmp/{str(fire_id)}'
                        sftp.chdir(output_directory)
                        print(f"Cambiando al directorio de salida: {output_directory}")
                        downloaded_files = []
                        for filename in sftp.listdir():
                                remote_file_path = os.path.join(output_directory, filename)
                                local_file_path = os.path.join(local_output_directory, filename)

                                # Descargar el archivo
                                sftp.get(remote_file_path, local_file_path)
                                print(f"Archivo {filename} descargado a {local_file_path}")
                                downloaded_files.append(local_file_path)
                
                        sftp.close()
                        print_directory_contents(local_output_directory)
                        local_output_directory = f'/tmp/{str(fire_id)}'
                        archivos_en_tmp = os.listdir(local_output_directory)
                        
                        key = uuid.uuid4()
                        for archivo in archivos_en_tmp:
                            archivo_path = os.path.join(local_output_directory, archivo)
                            if not os.path.isfile(archivo_path):
                                print(f"Skipping upload: {local_file_path} is not a file.")
                            else:
                                local_file_path = f"{mission_id}/{str(key)}"
                                upload_to_minio_path('minio_conn', 'missions', local_file_path, archivo_path)
                                output_data[archivo] = local_file_path

                # Y guardamos en la tabla de historico
                madrid_tz = pytz.timezone('Europe/Madrid')

                tipo2mesesminimo = int(Variable.get("tipo2mesesminimo", default_var="3"))
                fecha_proxima = FechaProxima()
                fecha_inicio = fecha_proxima.restar_meses(fecha_proxima.hoy, tipo2mesesminimo)
                fecha_hoy = fecha_proxima.hoy

                # Formatear phenomenon_time
                phenomenon_time = f"[{fecha_inicio.strftime('%Y-%m-%dT%H:%M:%S')}, {fecha_hoy.strftime('%Y-%m-%dT%H:%M:%S')}]"

                output_data["type"] = 2
                datos = {
                    'sampled_feature': mission_id, 
                    'result_time': datetime.datetime.now(madrid_tz),
                    'phenomenon_time': phenomenon_time,
                    'input_data': json.dumps({"fire_id": fire_id}),
                    'output_data': json.dumps(output_data)
                }

                # Construir la consulta de inserción
                query = f"""
                    INSERT INTO algoritmos.algoritmo_dnbr (
                        sampled_feature, result_time, phenomenon_time, input_data, output_data
                    ) VALUES (
                        {datos['sampled_feature']},
                        '{datos['result_time']}',
                        '{datos['phenomenon_time']}'::TSRANGE,
                        '{datos['input_data']}',
                        '{datos['output_data']}'
                    )
                """

                # Ejecutar la consulta
                execute_query('biobd', query)

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
