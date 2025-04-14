from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from airflow.providers.ssh.hooks.ssh import SSHHook
import requests
from dag_utils import execute_query,upload_to_minio_path,get_db_session,update_job_status, throw_job_error, crear_zip_si_existen
import os
import rasterio
from rasterio.features import rasterize
from pyproj import CRS
from shapely.geometry import shape
import geopandas as gpd
import uuid
import boto3
from botocore.client import Config
from airflow.hooks.base_hook import BaseHook
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta, timezone
from sqlalchemy import text
from rasterio.warp import calculate_default_transform, reproject, Resampling
import numpy as np
import pytz
from function_save_logs_to_minio import save_logs_to_minio


def process_escape_routes_data(**context):
    # Obtener los datos del contexto del DAG
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str)
    from_user = message['message']['from_user']
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')



    #Obtener urls de los path del algoritmo
    url = "https://actions-api.avincis.cuatrodigital.com/geo-files-locator/get-files-paths"  

    payload = [
        {
            "fileType": "carr_csv",
            "date": "2024-01-01",
            "location": "Galicia"
        },
        {
            "fileType": "combustible",
            "date": "2024-02-01",
            "location": "Galicia"
        }
        # {
        #     "fileType": "vias",
        #     "date": "2024-02-01",
        #     "location": "Galicia"
        # }

    ]

    headers = {
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        response.raise_for_status()  

        response_data = response.json()
        print("Datos obtenidos del endpoint de Hasura:")
        print(json.dumps(response_data, indent=4))

            # Crear un diccionario para almacenar los paths
        file_paths = {
            "dir_hojasmtn50": None,
            "dir_combustible": None,
            "dir_vias": None,
            "dir_cursos_agua": None,
            "dir_aguas_estancadas": None,
            "dir_carr_csv": None,
            "zonas_abiertas": None
        }

        for file in response_data:
            if "fileType" in file and "path" in file:
                if file["fileType"] == "hojasmtn50":
                    file_paths["dir_hojasmtn50"] = file["path"]
                elif file["fileType"] == "combustible":
                    file_paths["dir_combustible"] = file["path"]
                elif file["fileType"] == "vias":
                    file_paths["dir_vias"] = file["path"]
                elif file["fileType"] == "cursos_agua":
                    file_paths["dir_cursos_agua"] = file["path"]
                elif file["fileType"] == "aguas_estancadas":
                    file_paths["dir_aguas_estancadas"] = file["path"]
                elif file["fileType"] == "carr_csv":
                    file_paths["dir_carr_csv"] = file["path"]
                elif file["fileType"] == "zonas_abiertas":
                    file_paths["zonas_abiertas"] = file["path"]

    except requests.exceptions.RequestException as e:
        print(f"Error al llamar al endpoint de Hasura: {e}")
        error_message = str(e)
        print(f"Error durante el guardado de la misión: {error_message}")
        job_id = context['dag_run'].conf['message']['id']        
        throw_job_error(job_id, e)
        raise


    # Procesar "inicio" y "destino" para permitir diferentes estructuras
    inicio = input_data.get('inicio', None)
    destino = input_data.get('destino', None)

    if isinstance(inicio, str):
        try:
            inicio = json.loads(inicio)
        except json.JSONDecodeError:
            inicio = None

    if isinstance(destino, list):
        try:
            destino = [json.loads(d) if isinstance(d, str) else d for d in destino]
        except json.JSONDecodeError:
            destino = None
    elif isinstance(destino, dict):
        pass


    #Crear geojson con el incendio
    dir_incendio = input_data['dir_incendio'] 
    id_ruta = str(message['message']['id'])

    if isinstance(dir_incendio, str):
        dir_incendio = json.loads(dir_incendio)
        geojson = {
            "type": "FeatureCollection",
            "name": f"input_{id_ruta}_rutas_escape",
            "crs": {
                "type": "name",
                "properties": {
                    "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
                }
            },
            "features": [
                {
                    "type": "Feature",
                    "geometry": dir_incendio,
                    "properties": {
                        "CODIGO": f"ruta_{id_ruta}",
                        "SUPERFICIE": 0,  
                        "PERIMETRO": 0   
                    }
                }
            ]
        }

    geojson_file_path = f'/home/admin3/algoritmo_rutas_escape/input'
    json_file_path = f"{geojson_file_path}/input_{id_ruta}_rutas_escape.geojson"

    try:
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            print(f"Sftp abierto")

            print(f"Cambiando al directorio de lanzamiento y ejecutando limpieza de voluemnes")
            stdin, stdout, stderr = ssh_client.exec_command('cd /home/admin3/algoritmo_rutas_escape/launch && docker-compose down --volumes')

            container_name = "launch_alg_rutas_escape"
            stdin, stdout, stderr = ssh_client.exec_command(f"docker ps -a --filter name={container_name} --format '{{{{.ID}}}}'")
            container_id = stdout.read().decode().strip()

            if container_id:
                print(f"Contenedor encontrado: {container_id}. Eliminando...")
                # Detener y eliminar el contenedor
                ssh_client.exec_command(f"docker rm -f {container_id}")
                print(f"Contenedor {container_name} eliminado exitosamente.")
            else:
                print(f"No se encontró ningún contenedor con el nombre {container_name}.")
            
            ssh_client.exec_command(f"mkdir -p {geojson_file_path}")
            ssh_client.exec_command(f"chmod 777 {geojson_file_path}")

            ssh_client.exec_command(f"touch {json_file_path}")
            ssh_client.exec_command(f"chmod 644 {json_file_path}")

            with sftp.file(json_file_path, 'wb') as json_file:
                json_file.write(json.dumps(geojson, indent=4).encode('utf-8'))

            print(f"GeoJSON guardado en {geojson_file_path}")
            sftp.close()

    except Exception as e: 
        print(f"Error al guardar GeoJSON: {str(e)}")



    #,  "dir_incendio": f"{json_file_path}", "dir_carr_csv":  file_paths["dir_carr_csv"] file_paths["dir_combustible"]
    #        "dir_mdt": '/share_data/input/mdt05_hojasmtn50',
    #   "dir_hojasmtn50": "/share_data/input/cuadriculas/MTN50_ETRS89_Peninsula_Baleares_Canarias.shp",
    # Crear el JSON dinámicamente
    params = {
        "directorio_alg" : ".",
        "dir_output": f"/share_data/output/rutas_escape_{str(message['message']['id'])}",
        "dir_incendio": f"/share_data/input/input_{id_ruta}_rutas_escape.geojson",
        "dir_combustible": '/share_data/input/modelos_combustible_Galicia_2020.tif',
        "api_idee": input_data.get('api_idee', True),
        "dir_vias": "/share_data/input/ELEMENTOS/rt_tramo_vial.shp",
        "dir_cursos_agua": "/share_data/input/ELEMENTOS/cursosagua_Galicia.shp",
        "dir_aguas_estancadas": "/share_data/input/ELEMENTOS/aguas_estancadas_Galicia.shp",
        "inicio": inicio, 
        "destino": destino,  
        "direccion_avance": input_data.get('direccion_avance', None),
        "distancia": input_data.get('distancia', None),
        "dist_seguridad": input_data.get('dist_seguridad', None),
        "dir_obstaculos": input_data.get('dir_obstaculos', None),
        "dir_carr_csv": '/share_data/input/Tramos_de_carreteras.csv',
        "sugerir": input_data.get('sugerir', False),
        "zonas_abiertas": "/share_data/input/ZonasSeguras/Ourense.gpkg",
        "v_viento": input_data.get('v_viento', None),
        "f_buffer": input_data.get('f_buffer', 100),
        "c_prop": input_data.get('c_prop', "Extremas"),
        "lim_pendiente": input_data.get('lim_pendiente', None),
        "dist_estudio": input_data.get('dist_estudio', 5000),
        "write":input_data.get('write', True),
    }


    json_data = create_json(params)
    print(json_data)


    #Llamar al algoritmo
    try:
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            print(f"Sftp abierto")

            id_ruta = str(message['message']['id'])
            carpeta_destino = f'/home/admin3/algoritmo_rutas_escape/input'
            json_file_path = f'{carpeta_destino}/input_{id_ruta}.json'

            ssh_client.exec_command(f"touch {json_file_path}")
            ssh_client.exec_command(f"chmod 644 {json_file_path}")

            with sftp.file(json_file_path, 'w') as json_file:
                json_file.write(json.dumps(json_data, indent=4).encode('utf-8'))

   
            print(f"Archivo JSON guardado en: {json_file_path}")

            # Validar JSON
            with sftp.file(json_file_path, 'r') as json_file:
                json_data = json.load(json_file)
                print("Contenido del JSON válido:", json.dumps(json_data, indent=4))
                

            path = f'/share_data/input/input_{id_ruta}.json' 
            command = (
                f'cd /home/admin3/algoritmo_rutas_escape/scripts && CONFIGURATION_PATH={path} docker-compose -f ../launch/compose.yaml up --build'
            )

            stdin, stdout, stderr = ssh_client.exec_command(command)
            output = stdout.read().decode()
            error_output = stderr.read().decode()

            algorithm_error_message = None
            for line in output.split("\n"):
                if "Valor 100" in line or "Valor -2" in line or "Valor -1" in line:  
                    algorithm_error_message = line.strip()
                    print(f"Error durante el guardado de la misión: {algorithm_error_message}")
                    job_id = context['dag_run'].conf['message']['id']        
                    throw_job_error(job_id, algorithm_error_message)
                    raise Exception(algorithm_error_message)

            exit_status = stdout.channel.recv_exit_status() 
            if exit_status != 0:
                print("Errores al ejecutar run.sh:")
                print(error_output)

            print("Salida de docker:")
            print(output)


            output_directory = f'/home/admin3/algoritmo_rutas_escape/output/rutas_escape_{str(message['message']['id'])}' 

            local_output_directory = '/tmp'

            sftp.chdir(output_directory)
            print(f"Cambiando al directorio de salida: {output_directory}")

            downloaded_files = []
            for filename in sftp.listdir():
                if "ruta_escape" in filename:
                        remote_file_path = os.path.join(output_directory, filename)
                        local_file_path = os.path.join(local_output_directory, filename)

                        # Descargar el archivo
                        sftp.get(remote_file_path, local_file_path)
                        print(f"Archivo {filename} descargado a {local_file_path}")
                        downloaded_files.append(local_file_path)
            sftp.close()


            #Cerramos el algoritmo, leemos el resultado
            print_directory_contents(local_output_directory)

            local_output_directory = '/tmp'
            shapefile_path = os.path.join(local_output_directory, "ruta_escape.shp")
            tiff_output_path = os.path.join(local_output_directory, "ruta_escape.tiff")

            if(shapefile_path is None):
                print("Con los datos propuestos no se genero una ruta de escape")
                raise RuntimeError(f"Con los datos propuestos no se generó una ruta de escape")

            if(shapefile_path is not None):

                mission_id = input_data.get('mission_id', 'unknown_mission') 

                try:
                    outputZip = os.path.join(local_output_directory, "ruta_escape.zip")
                    crear_zip_si_existen("ruta_escape", local_output_directory, [".shp", ".prj", ".shx", ".dbf"])
                    key = f"{uuid.uuid4()}"
                    file_key = f'{mission_id}/escape_routes/{key}'
                    upload_to_minio_path('minio_conn', 'missions', file_key, outputZip)
                    file_url = f"https://minioapi.avincis.cuatrodigital.com/missions/{file_key}/ruta_escape.zip"
                    print(f" URL: {file_url}")
                except Exception as e:
                    print(f"Error al subir archivos a MinIO: {str(e)}")
                    raise e
    

        #Creamos la notificación de vuelta  
                try:
                    session = get_db_session()
                    engine = session.get_bind()
                    data_json = json.dumps({
                        "to": from_user,
                        "actions": [
                        {
                        "type": "notify",
                        "data": {
                            "message": "Datos de rutas de escape procesados correctamente"
                        }
                        },
                        {
                        "type": "paintShp",
                        "data": {
                            "url": file_url
                        }
                        }
                    ]
                    }, ensure_ascii=False)
                    time = datetime.now().replace(tzinfo=timezone.utc)

                    query = text("""
                        INSERT INTO public.notifications
                        (destination, "data", "date", status)
                        VALUES (:destination, :data, :date, NULL);
                    """)
                    session.execute(query, {
                        'destination': 'ignis',
                        'data': data_json,
                        'date': time
                    })
                    session.commit()

                except Exception as e:
                    session.rollback()
                    print(f"Error durante la inserción de la notificación: {str(e)}")
                    error_message = str(e)
                    print(f"Error durante el guardado de la misión: {error_message}")
                    # Actualizar el estado del job a ERROR y registrar el error
                    # Obtener job_id desde el contexto del DAG
                    job_id = context['dag_run'].conf['message']['id']        
                    throw_job_error(job_id, e)
                    raise e

            # Y guardamos en la tabla de historico
            madrid_tz = pytz.timezone('Europe/Madrid')
            



            datos = {
                'sampled_feature': input_data['mission_id'] ,  
                'result_time': datetime.now(madrid_tz),
                'phenomenon_time': datetime.now(madrid_tz),
                'input_data': json.dumps(params),  
                'output_data': json.dumps({"estado": "FINISHED", "zip_ficheros": file_url}),   
            }


            
            # Construir la consulta de inserción
            query = f"""
                INSERT INTO algoritmos.algoritmo_escape_routes (
                    sampled_feature, result_time, phenomenon_time, input_data, output_data
                ) VALUES (
                     {datos['sampled_feature']},
                    '{datos['result_time']}',
                    '{datos['phenomenon_time']}',
                    '{datos['input_data']}',
                    '{datos['output_data']}'
                )
            """

            # Ejecutar la consulta
            execute_query('biobd', query)



    except Exception as e:
        print(f"Error en el proceso: {str(e)}")
        error_message = str(e)
        print(f"Error durante el guardado de la misión: {error_message}")
        # Actualizar el estado del job a ERROR y registrar el error
        # Obtener job_id desde el contexto del DAG
        job_id = context['dag_run'].conf['message']['id']        
        throw_job_error(job_id, e)
        raise


    finally:
        # Cerrar SFTP si está abierto
        if 'sftp' in locals():
            sftp.close()
            print("SFTP cerrado.")
    # Actualizar el estado del job a 'FINISHED' si todo se completa correctamente
    try:
        job_id = message['message']['id']
        update_job_status(job_id, "FINISHED")

    except Exception as e:
        error_message = str(e)
        print(f"Error durante el guardado de la misión: {error_message}")
        # Actualizar el estado del job a ERROR y registrar el error
        # Obtener job_id desde el contexto del DAG
        job_id = context['dag_run'].conf['message']['id']        
        throw_job_error(job_id, e)
        raise
    



def reproject_tiff(input_tiff, output_tiff, dst_crs='EPSG:3857'):
    """
    Processes a TIFF file by scaling its values and reprojecting it to another CRS.
    
    Args:
        input_tiff (str): Path to the input TIFF file.
        output_tiff (str): Path to the output TIFF file.
        dst_crs (str): Target coordinate reference system (default: 'EPSG:3857').
    """
    # Ensure the output directory exists
    output_dir = os.path.dirname(output_tiff)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Open the original TIFF file
    with rasterio.open(input_tiff) as src:
        # Calculate the transformation and new size
        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds)
        
        # Display the source file profile
        print(src.profile)

        # Copy the metadata and update with the new parameters
        kwargs = src.meta.copy()

        kwargs.update({
            'crs': dst_crs,
            'transform': transform,
            'width': width,
            'height': height,
            'dtype': 'uint8',  # Change to uint8 data type
            'compress': 'lzw',  # LZW compression
            'predictor': 2,  # Compression predictor
            'zlevel': 3,  # Compression level
            'nodata': 0,  # Set nodata value
            'driver': 'GTiff'  # Output format GTiff
        })    

        # Open the output file for writing
        with rasterio.open(output_tiff, 'w', **kwargs) as dst:
            for i in range(1, src.count + 1):
                # Read the original band
                band_data = src.read(i)
                
                # Check for nodata values and handle them appropriately
                if src.nodata is not None:
                    band_data = np.where(band_data == src.nodata, 0, band_data)

                # Scale the values from 0-65535 to 0-255 (only for uint16)
                if band_data.dtype == 'uint16':
                    scaled_data = np.clip((band_data / 65535) * 255, 0, 255).astype('uint8')
                elif band_data.dtype == 'float32':
                    # Scale float32 values directly to 0-255 based on min and max values
                    scaled_data = np.clip((band_data - band_data.min()) / (band_data.max() - band_data.min()) * 255, 0, 255).astype('uint8')
                else:
                    # Handle other data types if necessary
                    scaled_data = band_data.astype('uint8')

                # Reproject the scaled band
                reproject(
                    source=scaled_data,
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.nearest
                )
        
        print(f"Reprojection complete. File saved at: {output_tiff}")



def print_directory_contents(directory):
    print(f"Contenido del directorio: {directory}")
    for root, dirs, files in os.walk(directory):
        level = root.replace(directory, '').count(os.sep)
        indent = ' ' * 4 * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print(f"{subindent}{f}")
    print("------------------------------------------")    



def create_json(params):
    # Generar un JSON basado en los parámetros proporcionados
    input_data = {
        "directorio_alg": params.get("directorio_alg", None),
        "dir_incendio": params.get("dir_incendio", None),
        "dir_mdt": params.get("dir_mdt", None),
        "dir_hojasmtn50": params.get("dir_hojasmtn50", None),
        "dir_combustible": params.get("dir_combustible", None),
        "api_idee": params.get("api_idee", False),
        "dir_vias": params.get("dir_vias", None),
        "dir_cursos_agua": params.get("dir_cursos_agua", None),
        "dir_aguas_estancadas": params.get("dir_aguas_estancadas", None),
        "inicio": params.get("inicio", None),
        "destino": params.get("destino", None),
        "direccion_avance": params.get("direccion_avance", None),
        "distancia": params.get("distancia", None),
        "dist_seguridad": params.get("dist_seguridad", None),
        "dir_obstaculos": params.get("dir_obstaculos", None),
        "dir_carr_csv": params.get("dir_carr_csv", None),
        "dir_output": params.get("dir_output", None),
        "sugerir": params.get("sugerir", False),
        "zonas_abiertas": params.get("zonas_abiertas", None),
        "v_viento": params.get("v_viento", None),
        "f_buffer": params.get("f_buffer", 100),
        "c_prop": params.get("c_prop", None),
        "lim_pendiente": params.get("lim_pendiente", None),
        "dist_estudio": params.get("dist_estudio", 5000),
        "write":params.get('write', True),
    }
    return input_data

def always_save_logs(**context):
    return True


# Configuración del DAG
default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'algorithm_escape_routes_post_process',
    default_args=default_args,
    description='DAG para generar JSON, mostrarlo por pantalla y actualizar estado del job',
    schedule_interval=None,
    catchup=False,
    concurrency=1
)


process_escape_routes_task = PythonOperator(
    task_id='process_escape_routes',
    provide_context=True,
    python_callable=process_escape_routes_data,
    dag=dag,
)

from utils.log_utils import setup_conditional_log_saving

check_logs, save_logs = setup_conditional_log_saving(
    dag=dag,
    task_id='save_logs_to_minio',
    task_id_to_save='process_escape_routes',
    condition_function=always_save_logs
)


process_escape_routes_task >> check_logs