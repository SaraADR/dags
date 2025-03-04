import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.hooks.base_hook import BaseHook
import json
from sqlalchemy import text
import requests
from dag_utils import upload_to_minio_path, print_directory_contents, execute_query
import uuid
import pytz

def process_element(**context):
    try:
        message = context["dag_run"].conf
        print(message)
        data = json.loads(message.get("data", "{}"))
        if not data.get("prediction", True):  # Por defecto True para evitar ejecución no deseada
            fire = data.get("fireId")
            perimeter = data.get("perimeterId")
            query = f"""
                SELECT mission_id
                FROM missions.mss_mission_fire mf
                WHERE mf.fire_id = {fire}
            """
            missionId = execute_query('biobd', query)         
            json_Perimetro = busqueda_datos_perimetro(fire, perimeter)
            print(json_Perimetro)
            ejecutar_algoritmo(data, missionId, fire)
        else:
            print("El campo 'prediction' es True, no se ejecuta el algoritmo.")
    except Exception as e:
        print(f"Error en la ejecución, el algoritmo ha dado un error en su salida")
    return


def ejecutar_algoritmo(params, mission_id, fire_id):
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')
    try:
        # Conectarse al servidor SSH
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            print(f"Sftp abierto")

            print(f"Cambiando al directorio de lanzamiento y ejecutando limpieza de volumenes")
            stdin, stdout, stderr = ssh_client.exec_command('cd /home/admin3/algoritmo_zonas_trabajo/launch && docker-compose down --volumes')
            stdout.channel.recv_exit_status()  # Esperar a que el comando termine

            if params is not None:                                    
                archivo_params = f"/home/admin3/algoritmo_zonas_trabajo/input/ejecucion.json"
                with sftp.file(archivo_params, 'w') as json_file:
                    json.dump(params, json_file, ensure_ascii=False, indent=4)
                    print(f"Guardado archivo {archivo_params}")
    

                path = f'/share_data/input/ejecucion.json' 
                stdin, stdout, stderr = ssh_client.exec_command(
                    f'cd /home/admin3/algoritmo_zonas_trabajo/scripts && '
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
                        
                output_directory = f'/home/admin3/algoritmo_dNBR/output/ejecucion'  
                local_output_directory = '/tmp'
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
            local_output_directory = '/tmp'
            archivos_en_tmp = os.listdir(local_output_directory)
            key = uuid.uuid4()
            for archivo in archivos_en_tmp:
                archivo_path = os.path.join(local_output_directory, archivo)
                if not os.path.isfile(archivo_path):
                    print(f"Skipping upload: {local_file_path} is not a file.")
                else:
                    local_file_path = f"{mission_id}/{str(key)}"
                    upload_to_minio_path('minio_conn', 'missions', local_file_path, archivo_path)
                    output_data[archivo] = local_file_path + '/' + archivo
            output_data["estado"] = "FINISHED"
    except Exception as e:
        print(f"Error en el proceso: {str(e)}")    
        output_data = {"estado": "ERROR", "comentario": str(e)}

    return 0


def historizacion(output_data, fire_id, mission_id):
    try:
            # Y guardamos en la tabla de historico
            madrid_tz = pytz.timezone('Europe/Madrid')

            # Calcular la fecha de inicio y fin
            fecha_hoy = datetime.datetime.now()
            fecha_inicio = fecha_hoy
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
 
def busqueda_datos_perimetro(idIncendio, idPerimetro):
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
                matching_perimeter = next((p for p in fire_data if p.get("id") == idPerimetro), None)

            if matching_perimeter:
                print("✅ Perímetro encontrado con éxito.")
                return matching_perimeter
            else:
                print(f"⚠ No se encontró un perímetro con ID {idPerimetro}.")
                return None

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