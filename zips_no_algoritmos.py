import json
import tempfile
import zipfile
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
import boto3
from botocore.client import Config
from airflow.hooks.base_hook import BaseHook
import os
from botocore.exceptions import ClientError
from airflow.providers.ssh.hooks.ssh import SSHHook
import re
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.orm import sessionmaker


def process_extracted_files(**kwargs):
    minio = kwargs['dag_run'].conf.get('minio')
    print(f"Mensaje: {minio}")

    if not minio:
        print("Ha habido un error con el traspaso de los documentos")
        return

    # Establecer conexión con MinIO
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

    # Nombre del bucket donde está almacenado el archivo/carpeta
    bucket_name = 'temp'
    folder_prefix = 'sftp/'

    # Descargar el archivo desde MinIO
    local_directory = 'temp'  # Cambia este path al local
    try:
        local_zip_path = download_from_minio(s3_client, bucket_name, minio, local_directory, folder_prefix)
        print(local_zip_path)
        process_zip_file(local_zip_path, minio, minio,  **kwargs)
    except Exception as e:
        print(f"Error al descargar desde MinIO: {e}")
        raise 
    return 0


def download_from_minio(s3_client, bucket_name, file_path_in_minio, local_directory, folder_prefix):
    """
    Función para descargar archivos o carpetas desde MinIO.
    """
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    local_file = os.path.join(local_directory, os.path.basename(file_path_in_minio))
    print(f"Descargando archivo desde MinIO: {file_path_in_minio} a {local_file}")
    relative_path = file_path_in_minio.replace('/temp/', '')

    try:
        # # Verificar si el archivo existe antes de intentar descargarlo
        response = s3_client.get_object(Bucket=bucket_name, Key=relative_path)
        with open(local_file, 'wb') as f:
            f.write(response['Body'].read())

        print(f"Archivo descargado correctamente: {local_file}")

        return local_file
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Error 404: El archivo no fue encontrado en MinIO: {file_path_in_minio}")
        else:
            print(f"Error en el proceso: {str(e)}")
        return None  # Devolver None si hay un error


def process_zip_file(local_zip_path, nombre_fichero, message, **kwargs):

    if local_zip_path is None:
        print(f"No se pudo descargar el archivo desde MinIO: {local_zip_path}")
        return
    

    try:
        if not os.path.exists(local_zip_path):
            print(f"Archivo no encontrado: {local_zip_path}")
            return
        

        # Abre y procesa el archivo ZIP desde el sistema de archivos
        with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
            zip_file.testzip() 
            print("El archivo ZIP es válido.")
    except zipfile.BadZipFile:
        print("El archivo no es un ZIP válido antes del procesamiento.")
        return
    

    try:
        with zipfile.ZipFile(local_zip_path, 'r') as zip_file:
            # Procesar el archivo ZIP en un directorio temporal
            with tempfile.TemporaryDirectory() as temp_dir:
                print(f"Directorio temporal creado: {temp_dir}")

                # Extraer el contenido del ZIP en el directorio temporal
                zip_file.extractall(temp_dir)

                # Obtener la lista de archivos dentro del ZIP
                file_list = zip_file.namelist()
                print("Archivos en el ZIP:", file_list)

                ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')
                try:
                    with ssh_hook.get_conn() as ssh_client:
                        sftp = ssh_client.open_sftp()

                        for file_name in file_list:
                    
                            if not file_name.endswith('/'):

                                local_file_path = os.path.join(temp_dir, file_name)
                                shared_volume_path = f"/home/admin3/exiftool/exiftool/images/{file_name}"

                                sftp.put(local_file_path, shared_volume_path)
                                print(f"Copied {local_file_path} to {shared_volume_path}")


                                # Execute Docker command for each file
                                docker_command = (
                                    f'cd /home/admin3/exiftool/exiftool && '
                                    f'docker run --rm -v /home/admin3/exiftool/exiftool:/images '
                                    f'--name exiftool-container-{file_name.replace(".", "-")} '
                                    f'exiftool-image -config /images/example2.0.0.txt -u /images/images/{file_name}'
                                )

                                stdin, stdout, stderr = ssh_client.exec_command(docker_command , get_pty=True)
                                output = ""
                                outputlimp = ""

                                for line in stdout:
                                    output += line.strip() + "\n"

                                print(f"Salida de docker command para {file_name}:")

                                # Clean up Docker container after each run
                                cleanup_command = f'docker rm exiftool-container-{file_name.replace(".", "-")}'
                                ssh_client.exec_command(cleanup_command)

                except Exception as e:
                    print(f"Error in SSH connection: {str(e)}")

                output_json = parse_output_to_json(output)
                save_data(output_json, message)

    except zipfile.BadZipFile as e:
        print(f"El archivo no es un ZIP válido: {e}")
        return

def parse_output_to_json(output):
    """
    Toma el output del comando docker como una cadena de texto y lo convierte en un diccionario JSON.
    """
    metadata = {}
    # Expresión regular para capturar pares clave-valor separados por ":"
    pattern = r"^(.*?):\s*(.*)$"
    for line in output.splitlines():
        match = re.match(pattern, line)
        if match:
            key = match.group(1).strip()
            key = key.strip().replace(" ", "_").lower()
            value = match.group(2).strip()
            metadata[key] = value
    
    return json.dumps(metadata, ensure_ascii=False, indent=4)


def save_data(data_json, message):
    print(data_json)
    data_json = json.loads(data_json)
    try:
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/v2.2"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()


        #DATOS DE OBSERVACION CAPTURA
        fecha_start = convert_timestamp(data_json.get("date_time_original", None)) if data_json.get("date_time_original") else None
        fecha_end = convert_timestamp(data_json.get("valid_time_end", None)) if data_json.get("valid_time_end") else None
        shape = 'SRID=' + data_json.get("image_coordinate_system") + ';POLYGON ((' +     '))'
        values_dict = {
            "fid": None,  #NO SE QUE VA AQUI
            "shape": shape,
            "sampled_feature": None,  #NO SE QUE VA AQUI
            "procedure": None,        #NO SE QUE VA AQUI
            "result_time": fecha_start, #NO SE QUE VA AQUI
            "phenomenon_time": fecha_end, #NO SE QUE VA AQUI
            "imagen": {
                "value": {
                    "url": message, #URL DE MINIO?
                    "gain": data_json.get("gain", None),
                    "focus": data_json.get("focus", None),
                    "width": data_json.get("image_width", None),
                    "height": data_json.get("image_height", None),
                    "aperture": data_json.get("aperture", None),
                    "vector_to": {"x": data_json.get("image_vector_to_x", 0), "y": data_json.get("image_vector_to_y", 0), "z": data_json.get("image_vector_to_z", 0)},
                    "vector_up": {"x": data_json.get("image_vector_up_x", 0), "y": data_json.get("image_vector_up_y", 0), "z": data_json.get("image_vector_up_z", 0)},
                    "gimbal_pan": data_json.get("gimbal_pan", None),
                    "quaternion": {
                        "quaternion0": data_json.get("image_quaternion_0", 0),
                        "quaternion1": data_json.get("image_quaternion_1", 0),
                        "quaternion2": data_json.get("image_quaternion_2", 0),
                        "quaternion3": data_json.get("image_quaternion_3", 0)
                    },
                    "gimbal_tilt": data_json.get("gimbal_tilt", None),
                    "focal_length":  data_json.get("focal_length", None),
                    "exposure_time": data_json.get("exposure_time", None),
                    "field_of_view": 0, #NO SE QUE VA AQUI
                    "camera_location": "SRID=4326;POINT(-7.718738 42.831722 789.4180952)",  #NO SE QUE VA AQUI
                    "classification0": 4, #NO SE QUE VA AQUI
                    "classification1": 3, #NO SE QUE VA AQUI
                    "classification2": 2, #NO SE QUE VA AQUI
                    "white_balance_b": data_json.get("white_balance_b", 0),
                    "white_balance_g": data_json.get("white_balance_g", 0),
                    "white_balance_r": data_json.get("white_balance_r", 0),
                    "camera_orientation": {
                        "yaw":  data_json.get("image_yaw", 0),
                        "roll":  data_json.get("image_roll", 0),
                        "pitch":  data_json.get("image_pitch", 0)
                    },
                    "geo_footprint_center": "SRID=4326;POINT(-7.718738 42.831722 12)",  #NO SE QUE VA AQUI
                    "ground_sample_distance":  data_json.get("ground_sampling_distance", 0)
                },
                "version": "2.0.0"
            }
        }

        # Imprimir el diccionario para verificar la salida
        print(values_dict)


        #Valores a introducir en captura imagen visible
        fecha_start = convert_timestamp(data_json.get("date_time_original", None)) if data_json.get("date_time_original") else None
        fecha_end = convert_timestamp(data_json.get("valid_time_end", None)) if data_json.get("valid_time_end") else None
        values_dict = {
            "fid": int(data_json.get("fid", 3)),
            "valid_time_start": fecha_start,
            "valid_time_end": fecha_end,
            "payload_id": data_json.get("payload_sn"),
            "multisim_id": data_json.get("multisim_sn"),
            "ground_control_station_id": data_json.get("ground_control_station_sn"),
            "pc_embarcado_id": data_json.get("pc_embarcado_sn"),
            "operator_name": data_json.get("operator_name"),
            "pilot_name": data_json.get("pilot_name"),
            "sensor": data_json.get("camera_model_name"),
            "platform": data_json.get("aircraft_number_plate")
        }
        print(values_dict)

        if (data_json.get("photometric_interpretation") == 'RGB'):
            metadata = MetaData(bind=engine)
            missions = Table('captura_imagen_visible', metadata, schema='observacion_aerea', autoload_with=engine)

        elif (data_json.get("photometric_interpretation") == 'BlackIsZero'):
            # TODO: CAMBIAR ESTA QUERY POR LA QUE TOQUE
            metadata = MetaData(bind=engine)
            missions = Table('captura_imagen_visible', metadata, schema='observacion_aerea', autoload_with=engine)

        else:
            # TODO: CAMBIAR ESTA QUERY POR LA QUE TOQUE
            metadata = MetaData(bind=engine)
            missions = Table('captura_imagen_visible', metadata, schema='observacion_aerea', autoload_with=engine)
        
        
        filtered_values = {key: value for key, value in values_dict.items() if value is not None}
        insert_stmt = missions.insert().values(**filtered_values)
        result = session.execute(insert_stmt)


        print(values_dict)
        print(result)

        try:
            # Confirma la transacción
            session.commit()
            print("Datos guardados en la base de datos con éxito.")
        except Exception as e:
            print(f"Error al guardar datos en la base de datos: {e}")
            session.rollback()  
        finally:
            session.close()

    except Exception as e:
        session.rollback()
        print(f"Error : {str(e)}")

    return json

def convert_timestamp(timestamp_str):
    try:
        return datetime.strptime(timestamp_str, '%Y:%m:%d %H:%M:%S%z') if timestamp_str else None
    except Exception as e:
        print(f"Error parsing timestamp: {e}")
        return None

default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'zips_no_algoritmos',
    default_args=default_args,
    description='DAG que lee todo lo que sea un zip pero no un algoritmo',
    catchup=False,
    schedule_interval=None
)

process_extracted_files_task = PythonOperator(
    task_id='process_extracted_files',
    python_callable=process_extracted_files,
    provide_context=True,
    dag=dag,
)




process_extracted_files_task