from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import uuid
import boto3
from botocore.client import Config
from airflow.hooks.base_hook import BaseHook
import os
from airflow.providers.postgres.operators.postgres import PostgresOperator
import os
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, Table, MetaData, text
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy.orm import sessionmaker
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
from airflow.providers.ssh.hooks.ssh import SSHHook
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
import numpy as np


def process_heatmap_data(**context):

    # Obtener el valor de 'type' de default_args a través del contexto
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str)
    task_type = message['message']['job']
    from_user = message['message']['from_user']

    isIncendio = "FALSE"
    arincendios = ''
    if task_type == 'heatmap-incendios':
        # Lógica específica para el heatmap de incendios
        print("Procesando datos para el heatmap de incendios.")
  
        # Modificaciones o lógica específica para incendios
        arincendios = "historical_fires.csv"
        isIncendio = "TRUE"
        # input_data["url_search_fire"] = "https://pre.atcservices.cirpas.gal/rest/FireService/searchByIntersection"
        # input_data["url_fireperimeter_service"] = "https://pre.atcservices.cirpas.gal/rest/FireAlgorithm_FirePerimeterService/getByFire?id="

    elif task_type == 'heatmap-aeronaves':
        # Lógica específica para el heatmap de aeronaves
        print("Procesando datos para el heatmap de aeronaves.")
        
        # Modificaciones o lógica específica para aeronaves
        arincendios = "historical_aircraft.csv"
        isIncendio = "FALSE"
        # input_data["url_search_aircraft"] = "https://pre.atcservices.cirpas.gal/rest/AircraftService/searchByIntersection"
        # input_data["url_aircraftperimeter_service"] = "https://pre.atcservices.cirpas.gal/rest/AircraftAlgorithm_AircraftPerimeterService/getByAircraft?id="
    
    
    # Convertir las fechas a datetime
    # low_search_date = datetime.strptime(input_data['lowSearchDate'], '%Y-%m-%dT%H:%M:%S.%fZ')
    # high_search_date = datetime.strptime(input_data['highSearchDate'], '%Y-%m-%dT%H:%M:%S.%fZ')

    # # Formatear las fechas al nuevo formato con zona horaria '+0000'
    # input_data['lowSearchDate'] = low_search_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0000'
    # input_data['highSearchDate'] = high_search_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0000'


    if 'lonlat' in input_data and len(input_data['lonlat']) == 4:
        lonlat = input_data.get('lonlat')
        if isIncendio == "FALSE":
            aircrafts_string = ", ".join(input_data.get('aircrafts', None))
        else:
            aircrafts_string = ''
        # Asignar los valores a minLon, maxLon, minLat, maxLat
        minLon = lonlat[0]
        maxLon = lonlat[2]
        minLat = lonlat[1]
        maxLat = lonlat[3]

        params = {
            "directorio_output":  '/share_data/output/' + str(task_type) + '_' + str(message['message']['id']),
            "incendios" : isIncendio,
            "ar_incendios": None,
            "comunidadAutonomaId":  input_data.get('comunidadId', None),
            "lowSearchDate" : input_data.get('lowSearchDate', None),
            "highSearchDate" : input_data.get('highSearchDate', None),
            "sigma" :  input_data.get('sigma', None),
            "codigo" : input_data.get('codigo', None),
            "title": aircrafts_string,
            "minlat": minLat,
            "maxlat": maxLat,
            "minlon": minLon,
            "maxlon": maxLon,
        }
    else:
        params = {
            "directorio_output":  '/share_data/output/' + str(task_type) + '_' + str(message['message']['id']),
            "incendios" : isIncendio,
            "ar_incendios": None,
            "comunidadAutonomaId":  input_data.get('comunidadId', None),
            "lowSearchDate" : input_data.get('lowSearchDate', None),
            "highSearchDate" : input_data.get('highSearchDate', None),
            "sigma" :  input_data.get('sigma', None),
            "codigo" : input_data.get('codigo', None)
        }
           
    print(params)
    # Generar el archivo JSON dinámicamente con los valores obtenidos
    json_file_path = create_json(params)


    #Guardar el json en la carpeta correspondiente
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')
    try:
        # Conectarse al servidor SSH
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            print(f"Sftp abierto")

            print(f"Cambiando al directorio de lanzamiento y ejecutando limpieza de voluemnes")
            stdin, stdout, stderr = ssh_client.exec_command('cd /home/admin3/Algoritmo_mapas_calor/algoritmo-mapas-de-calor-objetivo-1-master/launch && docker-compose down --volumes')
            
            output = stdout.read().decode()
            error_output = stderr.read().decode()

            print("Salida de docker volumes:")
            print(output)

            remote_directory = '/home/admin3/Algoritmo_mapas_calor/algoritmo-mapas-de-calor-objetivo-1-master/input'
            remote_file_name =  str(task_type) + '_' + str(message['message']['id']) + '.json'
            remote_file_path = os.path.join(remote_directory, remote_file_name)

            # Guardar los cambios de nuevo en el archivo
            with sftp.file(remote_file_path, 'w') as remote_file:
                json.dump(json_file_path, remote_file, indent=4)  
                print(f"Archivo {remote_file_name} actualizado en {remote_directory}")


            config_path = '/share_data/input/' +  str(task_type) + '_' + str(message['message']['id']) + '.json'
            command = f'cd /home/admin3/Algoritmo_mapas_calor/algoritmo-mapas-de-calor-objetivo-1-master/launch && CONFIGURATION_PATH={config_path} docker-compose -f compose.yaml up --build'
            stdin, stdout, stderr = ssh_client.exec_command(command)
            output = stdout.read().decode()
            error_output = stderr.read().decode()
            exit_status = stdout.channel.recv_exit_status() 

            print("Salida de docker:")
            print(output)

            if exit_status != 0:
                print("Errores al ejecutar run.sh:")
                print(error_output)
                message = context['dag_run'].conf
                job_id = message['message']['id']
                print(f"jobid {job_id}" )

                try:

                    # Conexión a la base de datos usando las credenciales almacenadas en Airflow
                    db_conn = BaseHook.get_connection('biobd')
                    connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
                    engine = create_engine(connection_string)
                    Session = sessionmaker(bind=engine)
                    session = Session()
                    metadata = MetaData(bind=engine)
                    jobs = Table('jobs', metadata, schema='public', autoload_with=engine)
                    
                    # Actualizar el estado del trabajo a "ERROR"
                    update_stmt = jobs.update().where(jobs.c.id == job_id).values(status='ERROR')
                    session.execute(update_stmt)
                    session.commit()
                    print(f"Job ID {job_id} status updated to ERROR")

                except Exception as e:
                    session.rollback()
                    print(f"Error durante el guardado del estado del job")

                # Lanzar la excepción para que la tarea falle
                raise RuntimeError(f"Error durante el guardado de la misión")

            

            else:
                output_directory = '/home/admin3//Algoritmo_mapas_calor/algoritmo-mapas-de-calor-objetivo-1-master/output/' + str(task_type) + '_' + str(message['message']['id'])
                local_output_directory = '/tmp' + '/' + str(message['message']['id'])

                # Crear el directorio local si no existe
                os.makedirs(local_output_directory, exist_ok=True)

                sftp.chdir(output_directory)
                print(f"Cambiando al directorio de salida: {output_directory}")
                downloaded_files = []
                for filename in sftp.listdir():
                    remote_file_path = os.path.join(output_directory, filename)
                    local_file_path = os.path.join(local_output_directory, filename)

                    # Descargar cada archivo
                    sftp.get(remote_file_path, local_file_path)
                    print(f"Archivo {filename} descargado a {local_file_path}")
                    downloaded_files.append(local_file_path)
            sftp.close()


            output_path = '/tmp/tiff_procesado.tif'
            if isIncendio == 'TRUE':
                input_path = local_output_directory + '/mapa_calor_incendios.tif'
            else:
                input_path = local_output_directory + '/mapa_calor_vuelos.tif'

            try:
                reproject_tiff(input_path, output_path)
            except Exception as e:
                print(f"Error en el proceso: {str(e)}")
                # try:

                #     # Conexión a la base de datos usando las credenciales almacenadas en Airflow
                #     db_conn = BaseHook.get_connection('biobd')
                #     connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
                #     engine = create_engine(connection_string)
                #     Session = sessionmaker(bind=engine)
                #     session = Session()
                #     metadata = MetaData(bind=engine)
                #     jobs = Table('jobs', metadata, schema='public', autoload_with=engine)
                    
                #     # Actualizar el estado del trabajo a "ERROR"
                #     update_stmt = jobs.update().where(jobs.c.id == job_id).values(status='ERROR')
                #     session.execute(update_stmt)
                #     session.commit()
                #     print(f"Job ID {job_id} status updated to ERROR")

                # except Exception as e:
                #     session.rollback()
                #     print(f"Error durante el guardado del estado del job")

            #Una vez tenemos lo que ha salido lo subimos a minio
            up_to_minio(local_output_directory, from_user, isIncendio, '/tmp')

    except Exception as e:
        print(f"Error en el proceso: {str(e)}")



def up_to_minio(local_output_directory, from_user, isIncendio, temp_dir):
    key = f"{uuid.uuid4()}"

    try:
        # Conexión a MinIO
        connection = BaseHook.get_connection('minio_conn')
        extra = json.loads(connection.extra)
        s3_client = boto3.client(
            's3',
            endpoint_url=extra['endpoint_url'],
            aws_access_key_id=extra['aws_access_key_id'],
            aws_secret_access_key=extra['aws_secret_access_key'],
            config=Config(signature_version='s3v4')
        )
        bucket_name = 'temp'
        
        # Listar todos los archivos en el directorio local de salida
        for filename in os.listdir(local_output_directory):
            local_file_path = os.path.join(local_output_directory, filename)
            
            print(filename)

            # Verificar que es un archivo
            if os.path.isfile(local_file_path) :
                # Generar un key único para cada archivo en MinIO
                file_key = f"{key}/{filename}"
                
                # Subir el archivo a MinIO
                s3_client.upload_file(local_file_path, bucket_name, file_key)
                print(f"Archivo {filename} subido correctamente a MinIO.")

        for filename in os.listdir(temp_dir):
            #Subir el tiff que sale del rasterio
            local_file_path = os.path.join(temp_dir, filename)
            
            print(filename)

            # Verificar que es un archivo
            if os.path.isfile(local_file_path) :
                # Generar un key único para cada archivo en MinIO
                file_key = f"{key}/{filename}"
                
                # Subir el archivo a MinIO
                s3_client.upload_file(local_file_path, bucket_name, file_key)
                print(f"Archivo {filename} subido correctamente a MinIO.")
                # Generar la URL del archivo subido           

                if filename.lower().endswith(('tiff_procesado.tif')):
                    file_url = f"https://minioapi.avincis.cuatrodigital.com/{bucket_name}/{file_key}"
                    print(f" URL: {file_url}")
                    
    except Exception as e:
        print(f"Error al subir archivos a MinIO: {str(e)}")



    try:
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

        data_json = json.dumps({
            "to": from_user,
            "actions": [
            {
            "type": "notify",
            "data": {
                "message": "Datos del heatmap procesados correctamente"
            }
            },
            {
            "type": "paintTiff",
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
    finally:
        session.close()




    # Subir el archivo TIFF a MinIO

    # cambiar_proyeccion_tiff(input_tiff=TIFF,output_tiff=TIFF2)
    # output_tiff = crear el directorio del output tiff con uuid 



    # tiff_key = f"{uuid.uuid4()}.tiff"
    # with tempfile.TemporaryDirectory() as temp_dir:
    #     temp_dir_file = os.path.join(temp_dir, tiff_key)

    #     reproject_tiff(algorithm_output_tiff, temp_dir_file)
    #     # input_data["temp_tiff_path"] = output_tiff
    



def change_state_job(**context):
    message = context['dag_run'].conf
    job_id = message['message']['id']
    print(f"jobid {job_id}" )

    try:
        # Conexión a la base de datos usando las credenciales almacenadas en Airflow
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

        # Update job status to 'FINISHED'
        metadata = MetaData(bind=engine)
        jobs = Table('jobs', metadata, schema='public', autoload_with=engine)
        update_stmt = jobs.update().where((jobs.c.id == job_id) & (jobs.c.status != 'ERROR')).values(status='FINISHED')
        session.execute(update_stmt)
        session.commit()
        print(f"Job ID {job_id} status updated to FINISHED")

    except Exception as e:
        session.rollback()
        print(f"Error durante el guardado del estado del job: {str(e)}")


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


def create_json(params):
    input_data = {
        "directorio_alg": params.get("directorio_alg", "."),
        "directorio_output": params.get("directorio_output"),
        "incendios": params.get("incendios", "FALSE"),
        "ar_incendios": params.get("ar_incendios", None),
        "url1": params.get("url1", None),
        "url2": params.get("url2", None),
        "url3": params.get("url3", None),
        "url4": params.get("url4", None),
        "user": params.get("user", "ITMATI.DES"),
        "password": params.get("password", "Cui_1234"),
        "minlat": params.get("minlat", None),
        "maxlat": params.get("maxlat", None),
        "minlon": params.get("minlon", None),
        "maxlon": params.get("maxlon", None),
        "comunidadAutonomaId": params.get("comunidadAutonomaId", None),
        "lowSearchDate": params.get("lowSearchDate", None),
        "highSearchDate": params.get("highSearchDate", None),
        "navstates": params.get("navstates", None),
        "title": params.get("title", None),
        "customerIds": params.get("customerIds", None),
        "res": params.get("res", None),
        "codigo": params.get("codigo", None),
        "sigma": params.get("sigma", None)
    }
    print("INPUT DATA")
    print(input_data)
    return input_data




# Configuración del DAG
default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'type': 'incendios',
}


# Definición del DAG incendios
dag = DAG(
    'process_heatmap',
    default_args=default_args,
    description='DAG para procesar datos de heatmap-incendio, subir TIFF a MinIO, y enviar notificaciones',
    schedule_interval=None,
    catchup=False,
    concurrency=1
)

# Tarea para el proceso de Heatmap de Incendios
process_heatmap_task = PythonOperator(
    task_id='process_heatmap_incendios',
    provide_context=True,
    python_callable=process_heatmap_data,
    dag=dag,
)

#Cambia estado de job
change_state_task = PythonOperator(
    task_id='change_state_job',
    python_callable=change_state_job,
    provide_context=True,
    dag=dag,
)

# Ejecución de la tarea en el DAG
process_heatmap_task >> change_state_task