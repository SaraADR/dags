import json
import os
import boto3
import re
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from botocore.client import Config
from airflow.operators.python_operator import PythonOperator
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.orm import sessionmaker


def process_extracted_files(**kwargs):

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
    folder_prefix = 'metadatos/'
    local_directory = 'temp'  

    try:
        # Listar objetos en la carpeta
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

        if 'Contents' not in response:
            print(f"No hay archivos en la carpeta {folder_prefix}")
            return
        
        for obj in response['Contents']:
            file_key = obj['Key']
            print(f"Procesando archivo: {file_key}")
            local_zip_path = download_from_minio(s3_client, bucket_name, file_key, local_directory, folder_prefix)
            process_zip_file(local_zip_path, file_key, file_key,  **kwargs)
        return  
    except Exception as e:
        print(f"Error al procesar los archivos: {e}")



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



def process_zip_file(local_zip_path, file_path, message, **kwargs):

    if local_zip_path is None:
        print(f"No se pudo descargar el archivo desde MinIO: {local_zip_path}")
        return
    name_short = os.path.basename(file_path)
    file_name = 'temp/' + name_short
    print(f"Ejecutando proceso de docker con el file {file_name}")
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')

    try:
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()


            shared_volume_path = f"/home/admin3/exiftool/exiftool/images/{name_short}"

            sftp.put(file_name, shared_volume_path)
            print(f"Copied {file_name} to {shared_volume_path}")


            # Execute Docker command for each file
            docker_command = (
                f'cd /home/admin3/exiftool/exiftool && '
                f'docker run --rm -v /home/admin3/exiftool/exiftool:/images '
                f'--name exiftool-container-{name_short.replace(".", "-")} '
                f'exiftool-image -config /images/example2.0.0.txt -u /images/images/{name_short}'
            )

            stdin, stdout, stderr = ssh_client.exec_command(docker_command , get_pty=True)
            output = ""
            outputlimp = ""

            for line in stdout:
                output += line.strip() + "\n"

            print(f"Salida de docker command para {file_name}:")
            print(output)

            # Clean up Docker container after each run
            cleanup_command = f'docker rm exiftool-container-{name_short.replace(".", "-")}'
            ssh_client.exec_command(cleanup_command)

    except Exception as e:
        print(f"Error in SSH connection: {str(e)}")

    output_json = parse_output_to_json(output)
    idRafaga = output_json.get("identificador_rafaga", 0)
    if(idRafaga != 0):
        #Es una rafaga
        is_rafaga(output, output_json)
    elif ( output_json.get("photometric_interpretation") == 'RGB'):
        #Es imagen visible
        is_visible_or_ter(output,output_json, 0)
    elif (output_json.get("photometric_interpretation") == 'BlackIsZero'):
        # Es termodinamica
        is_visible_or_ter(output,output_json, 1)
    else:
        print("No se reconoce el tipo de imagen o video aportado")
        return 





def is_rafaga(output, message):
    print("No se ha implementado el sistema de rafagas todavia")
    return

def is_visible_or_ter(output, output_json, type):
    if(type == 0):
        print("Vamos a ejecutar el sistema de guardados de imagenes visibles")
    if(type == 1):
        print("Vamos a ejecutar el sistema de guardados de imagenes infrarrojas")

    # Buscar los metadatos en captura
    try:
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/v2.2"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

        query = text("""
            SELECT fid
            FROM observacion_aerea.captura_imagen_visible
            WHERE (payload_id = :payload_id OR (payload_id IS NULL AND :payload_id IS NULL))
              AND (multisim_id = :multisim_id OR (multisim_id IS NULL AND :multisim_id IS NULL))
              AND (ground_control_station_id = :ground_control_station_id OR (ground_control_station_id IS NULL AND :ground_control_station_id IS NULL))
              AND (pc_embarcado_id = :pc_embarcado_id OR (pc_embarcado_id IS NULL AND :pc_embarcado_id IS NULL))
              AND (operator_name = :operator_name OR (operator_name IS NULL AND :operator_name IS NULL))
              AND (pilot_name = :pilot_name OR (pilot_name IS NULL AND :pilot_name IS NULL))
              AND (sensor = :sensor OR (sensor IS NULL AND :sensor IS NULL))
              AND (platform = :platform OR (platform IS NULL AND :platform IS NULL))
              AND (
                (:fecha_dada BETWEEN valid_time_start AND valid_time_end)
                OR (valid_time_start BETWEEN :one_hour_before AND :fecha_dada)
                OR (valid_time_end BETWEEN :fecha_dada AND :one_hour_after)
              )
        """)

        one_hour_before = output_json.get("date_time_original") - timedelta(hours=1)
        one_hour_after = output_json.get("date_time_original") + timedelta(hours=1)

        result = session.execute(query, {
            'payload_id': output_json.get("payload_sn"),
            'multisim_id': output_json.get("multisim_sn"),
            'ground_control_station_id': output_json.get("ground_control_station_sn"),
            'pc_embarcado_id': output_json.get("pc_embarcado_sn"),
            'operator_name': output_json.get("operator_name"),
            'pilot_name': output_json.get("pilot_name"),
            'sensor': output_json.get("camera_model_name"),
            'platform': output_json.get("aircraft_number_plate"),
            'fecha_dada': output_json.get("date_time_original"),
            'one_hour_before': one_hour_before,
            'one_hour_after': one_hour_after
        })
        print(result)

        row = result.fetchone()
        if row:
            print(row)
            return row['fid']
        else:
            print("No se encontró ningún registro que coincida.")
            return None


    except Exception as e:
        print(f"Error al descargar desde MinIO: {e}")
        raise 


    # # Guardamos en observacion_captura los datos encontrados
    # try:
    #     db_conn = BaseHook.get_connection('biobd')
    #     connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/v2.2"
    #     engine = create_engine(connection_string)
    #     Session = sessionmaker(bind=engine)
    #     session = Session()

    #     if (type == 0):
    #         metadata = MetaData(bind=engine)
    #         missions = Table('observacion_captura_imagen_visible', metadata, schema='observacion_aerea', autoload_with=engine)

    #     elif (type == 1):
    #         metadata = MetaData(bind=engine)
    #         missions = Table('observacion_captura_imagen_infrarroja', metadata, schema='observacion_aerea', autoload_with=engine)


    #     values_dict = {
    #         "shape": None,
    #         "sampled_feature": #Relacion con captura,
    #         "procedure": output_json.get("mission_id"),
    #         "result_time": ,
    #         "phenomenon_time": ,
    #         "imagen": output,
    #     }

    #     filtered_values = {key: value for key, value in values_dict.items() if value is not None}
    #     insert_stmt = missions.insert().values(**filtered_values)
    #     result = session.execute(insert_stmt)

    # except Exception as e:
    #     print(f"Error al descargar desde MinIO: {e}")
    #     raise 



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



default_args = {
    'owner': 'sadr',
    'depends_onpast': False,
    'start_date': datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'metadatos_mock',
    default_args=default_args,
    description='DAG procesa metadatos',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
)

process_extracted_files_task = PythonOperator(
    task_id='process_extracted_files',
    python_callable=process_extracted_files,
    provide_context=True,
    dag=dag,
)

process_extracted_files_task