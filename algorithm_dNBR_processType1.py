import base64
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
from sqlalchemy import text
import requests
from dag_utils import upload_to_minio_path, upload_to_geonetwork_xml,generate_dynamic_xml, publish_to_geoserver, generate_thumbnail, obtener_coordenadas_tif, execute_query
import uuid
from dateutil import parser

def process_element(**context):
    trace_id = str(uuid.uuid4())
    context['task_instance'].xcom_push(key='trace_id', value=trace_id)

    start_of_flow_task(trace_id, source='cron')

    madrid_tz = pytz.timezone('Europe/Madrid')
    fechaHoraActual = datetime.datetime.now(madrid_tz)  # Fecha y hora con zona horaria

    print(f"Este algoritmo se está ejecutando a las {fechaHoraActual.strftime('%Y-%m-%d %H:%M:%S')} en Madrid, España")

    print(f"Trace ID generado para esta ejecución: {trace_id}")

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
            stdout.channel.recv_exit_status() 

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
                print(f"incendio guardado en: {archivo_incendio}")

            if json_Perimetro is not None:                                    
                archivo_perimetro = f"/home/admin3/algoritmo_dNBR/input/perimetros/perimetro_{fire_id}_{fecha}.json"
                ssh_client.exec_command(f"touch {archivo_perimetro}")
                ssh_client.exec_command(f"chmod 644 {archivo_perimetro}")
                with sftp.file(archivo_perimetro, 'w') as json_file:
                    json_file.write(json.dumps(json_Perimetro, separators=(',', ':'), ensure_ascii=False).encode('utf-8'))
                print(f"perimetro guardado en: {archivo_perimetro}")

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
                    publish_files = [] 
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

                            with open(archivo_path, "rb") as f:
                                file_bytes = f.read()
                                publish_files.append({
                                    "file_name": archivo,
                                    "content": base64.b64encode(file_bytes).decode('utf-8')
                                })


                    
                    output_data["estado"] = "FINISHED"
                    print("----------- FINALIZACION DEL PROCESO PARA ESE INCENDIO  -------------------")
                    print(output_data)
                    print("-------------------------------------------")
                    historizacion(output_data, fire_id, mission_id )


                    #SUBIDA GEOSERVER
                    WORKSPACE = "Dnbr"
                    GENERIC_LAYER = "algoritm_dnbr"
                    wms_layers_info = publish_to_geoserver(publish_files, WORKSPACE, GENERIC_LAYER )
                    for wms in wms_layers_info:
                        filename = wms['filename']
                        if 'dNBR' in filename:
                            description = "Indice_DNBR. Capa ráster que representa el índice DNBR (Delta Normalized Burn Ratio), utilizado para evaluar la severidad de incendios mediante la comparación de imágenes multiespectrales pre y post incendio"
                        elif 'NDWI' in filename:
                            description = "Índice de agua normalizado"
                        elif 'clasificado' in filename:
                            description = "Clasificación de severidad de incendio"
                        elif 'erosion' in filename:
                            description = "Riesgo Erosion. Capa geoespacial que representa el riesgo potencial de erosión del suelo, basado en diversos factores"
                        elif 'perimetro' in filename:
                            description = "Perímetro. Capa vectorial que representa el perímetro del incendio calculado a partir del indice DNBR."
                        else:
                            description = f"Capa WMS para {filename}"
                        wms['description'] = description

                    print(wms_layers_info)

                    #SUBIDA GEONETWORK

                    nombre_incendio = json_Incendio.get('name', 'Nombre no disponible')
                    fecha_str = json_Incendio.get('start', None)
                    if fecha_str:
                        fecha_inicio_mision = parser.parse(fecha_str).strftime('%Y-%m-%d')
                    else:
                        fecha_inicio_mision = 'No disponible'

                    #info general
                    eiiob_titulo =  f"PostIncendio {nombre_incendio} {fecha_inicio_mision}"
                    eiiob_descripcion = "Conjunto de datos geoespaciales generados a partir del análisis de áreas afectadas por incendios forestales. Este conjunto puede incluir capas como el índice de severidad de incendio (dNBR), estimaciones del riesgo potencial de erosión y mapas de aptitud o potencial de revegetación natural. Los datos permiten evaluar el impacto del fuego sobre la vegetación y el suelo, y son útiles para apoyar la restauración ambiental, la planificación post-incendio y la gestión del territorio"
                    
                    #info tecnica
                    eiiob_inspire = "Zonas de riesgos naturales, Cubierta terrestre"
                    eiiob_categoria = "Cobertura de la Tierra con mapas básicos e imágenes Medio ambiente"
                    eiiob_pkey = "dNBR, Riesgo de erosión, Revegetación, Recuperación post-incendio, Degradación del suelo, Impacto ambiental"
                    eiiob_idioma = "Es"
                    eiiob_representacion = "Malla"
                    eiiob_referencia = "EPSG:4326"

                    bbox = None
                    images_files = []
                    for item in publish_files:
                        if item["file_name"] == "fire.dNBR.tif":
                            content_bytes = base64.b64decode(item["content"]) 
                            content_thumbnail, name_tumbnail = generate_thumbnail(content_bytes, item["file_name"])
                            bbox = obtener_coordenadas_tif(item["file_name"], content_bytes)
                            images_files.append({
                                "content": content_thumbnail, 
                                "file_name": name_tumbnail
                            })
                            break  
                    for item in publish_files:
                        if item["file_name"].lower().endswith(('.png', '.jpg', '.jpeg', '.gif')):
                            images_files.append({
                                "content": item["content"],  
                                "file_name": item["file_name"]
                        })


                    xml_data = generate_dynamic_xml(eiiob_titulo, eiiob_descripcion, eiiob_inspire, eiiob_categoria,eiiob_pkey,eiiob_idioma,eiiob_representacion, eiiob_referencia, bbox, wms_layers_info, images_files, mission_id)
                    resources_id = upload_to_geonetwork_xml(xml_data)
                    


    except Exception as e:
        print(f"Error en el proceso: {str(e)}")    
        output_data = {"estado": "ERROR", "comentario": str(e)}  

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
 

def busqueda_datos_perimetro(idIncendio):
        try:
            print("Buscando el perimetro del incendio en einforex")
            # Conexión al servicio ATC usando las credenciales almacenadas en Airflow
            conn = BaseHook.get_connection('atc_services_connection')
            auth = (conn.login, conn.password)
            url = f"{conn.host}/rest/FireAlgorithm_FirePerimeterService/getByFire?id={idIncendio}"

            response = requests.get(url, auth=auth)

            if response.status_code == 200:
                fire_data = response.json()
                if fire_data:
                        most_recent_obj = max(fire_data, key=lambda x: x["timestamp"])
                        print("Perimetros del incendio encontrados con exito.")
                        return most_recent_obj
                else:
                    print("⚠️ No se encontraron datos de perímetro en la respuesta.")
                    raise Exception(f"No hay perimetro para el incendio: {response.status_code}")
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