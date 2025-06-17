import datetime
import os
import tempfile
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
from dag_utils import  upload_to_minio_path, print_directory_contents
import uuid
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def process_element(**context):

    madrid_tz = pytz.timezone('Europe/Madrid')
    fechaHoraActual = datetime.datetime.now(madrid_tz)  # Fecha y hora con zona horaria

    print(f"Este algoritmo se está ejecutando a las {fechaHoraActual.strftime('%Y-%m-%d %H:%M:%S')} en Madrid, España")

    tipo1diasincendio = Variable.get("dNBR_diasFinIncendio", default_var="10")
    print(f"Valor de la variable tipo1diasincendio en Airflow: {tipo1diasincendio}")

    interval_value = f'{tipo1diasincendio} days'
    query = f"""
        SELECT mf.fire_id, m.id
        FROM missions.mss_mission m
        JOIN missions.mss_mission_fire mf ON m.id = mf.mission_id
        WHERE mf.extinguishing_timestamp::DATE = (CURRENT_DATE - INTERVAL '{interval_value}')
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
    print(datos)


    try:
        # Conectarse al servidor SSH
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            print(f"Sftp abierto")

            print(f"Cambiando al directorio de lanzamiento y ejecutando limpieza de volumenes")
            stdin, stdout, stderr = ssh_client.exec_command('cd /home/admin3/algoritmo_dNBR/launch && docker-compose down --volumes')
            stdout.channel.recv_exit_status()  # Esperar a que el comando termine

            fire_id, mission_id = datos
            print(f"dato: {datos}")
            json_Incendio = busqueda_datos_incendio(fire_id)
            json_Perimetro = busqueda_datos_perimetro(fire_id)

            print(json_Incendio)
            print(json_Perimetro)

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
                    json_file.write(json.dumps(json_Perimetro, separators=(',', ':'), ensure_ascii=False).encode('utf-8'))

            params = {
                "directorio_alg":  '.',
                "directorio_output" : '/share_data/output/' + str(fire_id) + "_" + str(fecha),
                "obj_incendio":  f'/share_data/input/ob_incendio/incendio_{fire_id}_{fecha}.json',
                "obj_perimetro":  f'/share_data/input/perimetros/perimetro_{fire_id}_{fecha}.json',
                "service_account" : Variable.get("dNBR_path_serviceAccount", default_var=None), 
                "credenciales" : '/share_data/input/algoritmos-bio-b40e24394020.json',
                "dias_pre" :  int(Variable.get("dNBR_diasPre", default_var=10)),
                "dias_post" : int(Variable.get("dNBR_diasPost", default_var=10)),
                "combustibles" : Variable.get("dNBR_pathCombustible", default_var="/share_data/input/galicia_mod_com_filt.tif"),
                "buffer":  int(Variable.get("dNBR_buffer", default_var=1200))
            }

            print(params)
            output_data = {}
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

                print("Salida de run.sh:")
                print(output)
                for line in output.split("\n"):
                    if "Valor -3: La región del incendio no se incluye en la capa de combustibles." in line or "Valor -1: No se pudo generar una imagen" in line or "Valor -100" in line: 
                        algorithm_error_message = line.strip()
                        print(f"Error durante el guardado de la misión: {algorithm_error_message}")
                        output_data = {"estado": "ERROR", "comentario": algorithm_error_message}
                        historizacion(output_data, fire_id, mission_id )
                        raise Exception(algorithm_error_message)
                        
               
               
                output_directory = f'/home/admin3/algoritmo_dNBR/output/' + str(fire_id) + "_" + str(fecha) 
                with tempfile.TemporaryDirectory() as lod:                  
                    sftp.chdir(output_directory)
                    print(f"Cambiando al directorio de salida: {lod}")
                    downloaded_files = []
                    for filename in sftp.listdir():
                        remote_file_path = os.path.join(output_directory, filename)
                        local_file_path = os.path.join(lod, filename)

                        # Descargar el archivo
                        sftp.get(remote_file_path, local_file_path)
                        print(f"Archivo {filename} descargado a {local_file_path}")
                        downloaded_files.append(local_file_path)
            
                    sftp.close()
                    archivos_en_tmp = os.listdir(lod)
                    output_data = {}
                    key = uuid.uuid4()

                    for archivo in archivos_en_tmp:
                        archivo_path = os.path.join(lod, archivo)
                        if not os.path.isfile(archivo_path):
                            print(f"Skipping upload: {local_file_path} is not a file.")
                        else:
                            local_file_path = f"{mission_id}/{str(key)}"
                            upload_to_minio_path('minio_conn', 'missions', local_file_path, archivo_path)
                            output_data[archivo] = local_file_path + '/' + archivo


                    
                    output_data["estado"] = "FINISHED"
                    print("----------- FINALIZACION DEL PROCESO PARA ESE INCENDIO  -------------------")
                    print(output_data)
                    print("-------------------------------------------")
                    historizacion(output_data, fire_id, mission_id )

    except Exception as e:
        print(f"Error en el proceso: {str(e)}")    
        output_data = {"estado": "ERROR", "comentario": str(e)}

    return None

def historizacion(output_data, fire_id, mission_id):
    try:
            # Y guardamos en la tabla de historico
            madrid_tz = pytz.timezone('Europe/Madrid')
            tipo1diasincendio = int(Variable.get("dNBR_diasFinIncendio", default_var="10"))

            # Calcular la fecha de inicio y fin
            fecha_hoy = datetime.datetime.now()
            fecha_inicio = fecha_hoy - datetime.timedelta(days=tipo1diasincendio)
            phenomenon_time = f"[{fecha_inicio}, {fecha_hoy}]"

            output_data["type"] = 1
            datos = {
                'sampled_feature': mission_id,  # Ejemplo de valor
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
    except Exception as e:
        print(f"Error en el proceso: {str(e)}")  
    return  
 

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

trigger_monitoring = TriggerDagRunOperator(
    task_id="trigger_monitor_dags",
    trigger_dag_id="monitor_dags",  
    conf={"dag_name": dag.dag_id}, 
    dag=dag,
)


process_element_task >> trigger_monitoring