import json
import os
import shutil
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


#TRAE TODOS LOS FICHEROS DE LA CARPETA DE MINIO
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



#HACE LECTURA DE METADATOS Y CONTROLA EL TIPO ENCONTRADO
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

            for line in stdout:
                output += line.strip() + "\n"

            print(f"Salida de docker command para {file_name}:")
            print(output)

            # Clean up Docker container after each run
            cleanup_command = f'docker rm exiftool-container-{name_short.replace(".", "-")}'
            ssh_client.exec_command(cleanup_command)

    except Exception as e:
        print(f"Error in SSH connection: {str(e)}")

    #CONTROL DEL TIPO ENCONTRADO
    output_json_noload = parse_output_to_json(output)
    output_json = json.loads(output_json_noload)


    idRafaga = output_json.get("identificador_rafaga", '0')

    print(message)
    # if(idRafaga != '0'):
    #     #Es una rafaga
    #     is_rafaga(output, output_json)


    # #SON VIDEOS
    # elif ".mp4" in message :
    #     output_json_comment = json.loads(output_json.get("comment"))
    #     print(output_json_comment)
    #     is_visible_or_ter(output,output_json_comment, -1)
    # #SON IMAGENES
    # elif "-vis" in message:
    #     #Es imagen visible
    #     is_visible_or_ter(output,output_json, 0)
    # elif "-ter" in message:
    #     # Es termodinamica
    #     is_visible_or_ter(output,output_json, 1)
    # elif "-mul" in message:
    #     # Es termodinamica
    #     is_visible_or_ter(output,output_json, 2)
    # else:
    #     if output_json.get("sensor_id") == 1:
    #          is_visible_or_ter(output,output_json, 0)
    #     elif output_json.get("sensor_id") == 2:
    #          is_visible_or_ter(output,output_json, 1)
    #     else:
    #         is_visible_or_ter(output,output_json, 0)
    #         print("No se reconoce el tipo de imagen o video aportado")
    #     return 
    return


#PROCEDIMIENTO A LLEVAR CON LAS RAFAGAS
def is_rafaga(output, message):
    print("No se ha implementado el sistema de rafagas todavia")
    return



#PROCEDIMIENTO A LLEVAR CON INDIVIDUALES
def is_visible_or_ter(output, output_json, type):

    if(type == 0):
        print("Vamos a ejecutar el sistema de guardados de imagenes visibles")
        table_name = "observacion_aerea.captura_imagen_visible"
    if(type == 1):
        print("Vamos a ejecutar el sistema de guardados de imagenes infrarrojas")
        table_name = "observacion_aerea.captura_imagen_infrarroja"
    if(type == 2):
        print("Vamos a ejecutar el sistema de guardados de imagenes multiespectral")
        table_name = "observacion_aerea.captura_imagen_multiespectral"
    if(type == -1):
        print("Vamos a ejecutar el sistema de guardados de imagenes multiespectral")
        table_name = "observacion_aerea.captura_video"     

    # Buscar los metadatos en captura
    try:
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/v2.3"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

        #QUERY PARA BUSQUEDAS
        query = text(f"""
            SELECT fid, valid_time
            FROM {table_name}
            WHERE (payload_id = :payload_id OR (payload_id IS NULL AND :payload_id IS NULL))
            AND (fid = :fid)
            AND (multisim_id = :multisim_id OR (multisim_id IS NULL AND :multisim_id IS NULL))
            AND (ground_control_station_id = :ground_control_station_id OR (ground_control_station_id IS NULL AND :ground_control_station_id IS NULL))
            AND (pc_embarcado_id = :pc_embarcado_id OR (pc_embarcado_id IS NULL AND :pc_embarcado_id IS NULL))
            AND (operator_name = :operator_name OR (operator_name IS NULL AND :operator_name IS NULL))
            AND (pilot_name = :pilot_name OR (pilot_name IS NULL AND :pilot_name IS NULL))
            AND (sensor = :sensor OR (sensor IS NULL AND :sensor IS NULL))
            AND (platform = :platform OR (platform IS NULL AND :platform IS NULL))
            AND (
                tsrange(lower(valid_time) - INTERVAL '2 HOURS', upper(valid_time) + INTERVAL '2 HOURS', '[)') @> :fecha_dada
            )
        """)

        date_time_str = output_json.get("date/time_original")
        try:
            date_time_original = datetime.strptime(date_time_str, "%Y:%m:%d %H:%M:%S")
        except ValueError as ve:
            print(f"Error al parsear la fecha '{date_time_str}': {ve}")
            raise

        if(type == -1):
            # result = session.execute(query, {
            #     'fid' :  output_json.get("sensor_id"),   
            #     'payload_id': output_json.get("payload_sn"),
            #     'multisim_id': output_json.get("multisim_sn"),
            #     'ground_control_station_id': output_json.get("ground_control_station_sn"),
            #     'pc_embarcado_id': output_json.get("pc_embarcado_sn"),
            #     'operator_name': output_json.get("operator_name"),
            #     'pilot_name': output_json.get("pilot_name"),
            #     'sensor': output_json.get("camera_model_name"),
            #     'platform': output_json.get("aircraft_number_plate"),
            #     'fecha_dada': date_time_original
            # })
            print (output_json)
            return
        else:

            result = session.execute(query, {
                'fid' :  output_json.get("sensor_id"),   
                'payload_id': output_json.get("payload_sn"),
                'multisim_id': output_json.get("multisim_sn"),
                'ground_control_station_id': output_json.get("ground_control_station_sn"),
                'pc_embarcado_id': output_json.get("pc_embarcado_sn"),
                'operator_name': output_json.get("operator_name"),
                'pilot_name': output_json.get("pilot_name"),
                'sensor': output_json.get("camera_model_name"),
                'platform': output_json.get("aircraft_number_plate"),
                'fecha_dada': date_time_original
            })

        row = result.fetchone()
        
        #SE COMRPUEBA SI SE ACTUALIZA O CREA NUEVA
        if row:
            print(f"row: {row['fid']}")
            fid = row['fid']
            valid_time = row['valid_time']  
            
            if not (date_time_original in valid_time):  # Verifica si la fecha no está en el rango actual
                if date_time_original < valid_time.lower:  # Fecha antes del inicio del rango
                    diferencia = valid_time.lower - date_time_original
                    print(f"Fecha dada {date_time_original} está antes del inicio ({valid_time.lower}) en {diferencia} (h:m:s), se actualiza el inicio del rango.")

                    # Actualizar el inicio del rango
                    update_query = text(f"""
                        UPDATE {table_name}
                        SET valid_time = tsrange(:new_start, upper(valid_time), '[)')
                        WHERE payload_id = :payload_id
                        AND multisim_id = :multisim_id
                        AND ground_control_station_id = :ground_control_station_id
                        AND pc_embarcado_id = :pc_embarcado_id
                        AND operator_name = :operator_name
                        AND pilot_name = :pilot_name
                        AND sensor = :sensor
                        AND platform = :platform
                        AND fid = :fid
                    """)
                    session.execute(update_query, {
                        "new_start": date_time_original,
                        "fid": fid,
                        'payload_id': output_json.get("payload_sn"),
                        'multisim_id': output_json.get("multisim_sn"),
                        'ground_control_station_id': output_json.get("ground_control_station_sn"),
                        'pc_embarcado_id': output_json.get("pc_embarcado_sn"),
                        'operator_name': output_json.get("operator_name"),
                        'pilot_name': output_json.get("pilot_name"),
                        'sensor': output_json.get("camera_model_name"),
                        'platform': output_json.get("aircraft_number_plate"),
                    })

                elif date_time_original > valid_time.upper:  # Fecha después del final del rango
                    diferencia = date_time_original - valid_time.upper
                    print(f"Fecha dada {date_time_original} está después del final ({valid_time.upper}) en {diferencia} (h:m:s), se actualiza el final del rango.")

                    # Actualizar el final del rango
                    update_query = text(f"""
                        UPDATE {table_name}
                        SET valid_time = tsrange(lower(valid_time), :new_end, '[)')
                        WHERE payload_id = :payload_id
                        AND multisim_id = :multisim_id
                        AND ground_control_station_id = :ground_control_station_id
                        AND pc_embarcado_id = :pc_embarcado_id
                        AND operator_name = :operator_name
                        AND pilot_name = :pilot_name
                        AND sensor = :sensor
                        AND platform = :platform
                        AND fid = :fid
                    """)
                    session.execute(update_query, {
                        "new_end": date_time_original,
                        "fid": fid,
                        'payload_id': output_json.get("payload_sn"),
                        'multisim_id': output_json.get("multisim_sn"),
                        'ground_control_station_id': output_json.get("ground_control_station_sn"),
                        'pc_embarcado_id': output_json.get("pc_embarcado_sn"),
                        'operator_name': output_json.get("operator_name"),
                        'pilot_name': output_json.get("pilot_name"),
                        'sensor': output_json.get("camera_model_name"),
                        'platform': output_json.get("aircraft_number_plate"),
                    })
            else:
                print(f"Fecha dada {date_time_original} está dentro del rango {valid_time}, no se realiza actualización.")


        else:
            print("No se encontró ningún registro que coincida, se procede a incluir la linea")

            insert_query = text(f"""
                INSERT INTO {table_name}
                (fid, valid_time, payload_id, multisim_id, 
                ground_control_station_id, pc_embarcado_id, operator_name, pilot_name, 
                sensor, platform)
                VALUES (:fid, tsrange(:valid_time_start, :valid_time_end, '[)'), :payload_id, :multisim_id, 
                        :ground_control_station_id, :pc_embarcado_id, :operator_name, :pilot_name, 
                        :sensor, :platform)
            """)

            # Generar los valores de inserción
            insert_values = {
                'fid': int(output_json.get("sensor_id")),
                'valid_time_start': date_time_original,
                'valid_time_end': date_time_original + timedelta(minutes=1),  # Ajusta el rango inicial
                'payload_id': output_json.get("payload_sn"),
                'multisim_id': output_json.get("multisim_sn"),
                'ground_control_station_id': output_json.get("ground_control_station_sn"),
                'pc_embarcado_id': output_json.get("pc_embarcado_sn"),
                'operator_name': output_json.get("operator_name"),
                'pilot_name': output_json.get("pilot_name"),
                'sensor': output_json.get("camera_model_name"),
                'platform': output_json.get("aircraft_number_plate")
            }
            print(insert_values)
            session.execute(insert_query, insert_values)
            session.commit()


        #INSERTAMOS EN OBSERVACION CAPTURA LA IMAGEN

        if (type == 0): #Es una visible
            table_name_observacion = "observacion_aerea.observation_captura_imagen_visible"
        if (type == 1): #Es una infrarroja
            table_name_observacion = "observacion_aerea.observation_captura_imagen_infrarroja"
        if (type == 2): #Es una multiespectral
            table_name_observacion = "observacion_aerea.observation_captura_imagen_multiespectral"

        insert_query = text(f"""
            INSERT INTO {table_name_observacion}
            ( shape, sampled_feature, procedure, result_time, phenomenon_time, imagen)
            VALUES ( :shape, :sampled_feature, :procedure, :result_time, 
                :phenomenon_time, :imagen)
        """)
     

        shape = generar_shape_con_offsets(output_json)
        print(shape)
        insert_values = {
            "shape": shape,
            "sampled_feature": output_json.get("mission_id"),
            "procedure": int(output_json.get("sensor_id")),
            "result_time":  date_time_original,
            "phenomenon_time": date_time_original,
            "imagen": parse_output_to_json_clean(output),
        }

        session.execute(insert_query, insert_values)
        session.commit()
        session.close()

    except Exception as e:
        print(f"Error al introducir la linea en observacion captura: {e}")
        raise 





# METODOS AUXILIARES

def generar_shape_con_offsets(data):

    gps_lat = data.get("gps_latitude")
    gps_long = data.get("gps_longitude")
    
    # Convertir coordenadas de strings a valores numéricos
    lat_central = float(gps_lat.split()[0]) * (1 if gps_lat.split()[1] == "N" else -1)
    long_central = float(gps_long.split()[0]) * (1 if gps_long.split()[1] == "E" else -1)

    offsets = [
        data.get("offset_corner_longitude_point_1", 0),
        data.get("offset_corner_latitude_point_2", 0),
        data.get("offset_corner_longitude_point_2", 0),
        data.get("offset_corner_latitude_point_3", 0),
        data.get("offset_corner_longitude_point_3", 0),
        data.get("offset_corner_latitude_point_4", 0),
        data.get("offset_corner_longitude_point_4", 0),
        data.get("offset_corner_latitude_point_2", 0)  
    ]
    offsets = [float(offset) if offset else 0 for offset in offsets]


    vertices = []
    for i in range(0, len(offsets), 2):
        offset_long = offsets[i]
        offset_lat = offsets[i + 1]
        vertex_long = long_central + offset_long
        vertex_lat = lat_central + offset_lat
        vertices.append((vertex_long, vertex_lat))

    if vertices[-1] != vertices[0]:
        vertices.append(vertices[0])

    vertices_str = ", ".join(f"{lon} {lat}" for lon, lat in vertices)
    shape = f"SRID=4326;POLYGON (({vertices_str}))"
    return shape


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

def parse_output_to_json_clean(output):
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
            key = key.strip()
            value = match.group(2).strip()
            metadata[key] = value
    
    return json.dumps(metadata, ensure_ascii=False, indent=4)


#DESCARGA CADA UNO DE LOS FICHEROS DE MANERA INDIVIDUAL
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



def clean_temp_directory():
    temp_dir = "temp"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        print(f"Directorio {temp_dir} limpiado exitosamente.")
    else:
        print(f"El directorio {temp_dir} no existe, no se requiere limpieza.")
        

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
    'test_metadatos_mock',
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

    # Tarea de limpieza
cleanup_task = PythonOperator(
        task_id='cleanup_temp',
        python_callable=clean_temp_directory,
)


process_extracted_files_task >> cleanup_task