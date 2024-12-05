import json
import os
import shutil
import boto3
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from botocore.client import Config
from airflow.operators.python_operator import PythonOperator
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.orm import sessionmaker
from dag_utils import dms_to_decimal, duration_to_seconds, parse_output_to_json, upload_to_minio, upload_to_minio_path

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
                f'exiftool-image -config /images/example2.0.0.txt -u -s /images/images/{name_short}'
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
        print(f"Error en la conexión SSH o en el procesamiento Docker: {str(e)}")
        return

    #CONTROL DEL TIPO ENCONTRADO
    output_json_noload, comment_json = parse_output_to_json(output)
    print(output_json_noload)
    print(comment_json)
    output_json = json.loads(output_json_noload)
    


    idRafaga = output_json.get("IdentificadorRafaga", '0')
    if(idRafaga != '0'):
        #Es una rafaga
        is_rafaga(output, output_json)
    #SON VIDEOS
    elif message.endswith(".mp4"):
        comments = json.loads(comment_json)
        is_visible_or_ter(message, local_zip_path, output_json_noload, comments, -1)
    #SON IMAGENES
    elif "-vis" in message:
        #Es imagen visible
        is_visible_or_ter(message,local_zip_path, output_json_noload, output_json, 0)
    elif "-ter" in message:
        # Es termodinamica
        is_visible_or_ter(message,local_zip_path,output_json_noload,output_json, 1)
    elif "-mul" in message:
        # Es termodinamica
        is_visible_or_ter(message,local_zip_path,output_json_noload,output_json, 2)
    else:
        if output_json.get("sensor_id") == 1:
             is_visible_or_ter(message,local_zip_path,output_json_noload,output_json, 0)
        elif output_json.get("sensor_id") == 2:
             is_visible_or_ter(message,local_zip_path,output_json_noload,output_json, 1)
        else:
            is_visible_or_ter(message,local_zip_path, output_json_noload,output_json, 0)
            print("No se reconoce el tipo de imagen o video aportado")
        return 
    return


##--------------------------- PROCEDIMIENTO DE RAFAGAS ------------------------------------------------
def is_rafaga(output, message):
    print("No se ha implementado el sistema de rafagas todavia")
    return



##--------------------------- PROCEDIMIENTO DE IMAGENES Y VIDEOS ---------------------------------------
def is_visible_or_ter(message, local_zip_path, output, output_json, type):

    if(type == 0):
        print("Vamos a ejecutar el sistema de guardados de imagenes visibles")
        table_name = "observacion_aerea.captura_imagen_visible"
        SensorId = output_json.get("SensorID")    
    if(type == 1):
        print("Vamos a ejecutar el sistema de guardados de imagenes infrarrojas")
        table_name = "observacion_aerea.captura_imagen_infrarroja"
        SensorId = output_json.get("SensorID")    
    if(type == 2):
        print("Vamos a ejecutar el sistema de guardados de imagenes multiespectral")
        table_name = "observacion_aerea.captura_imagen_multiespectral"
        SensorId = output_json.get("SensorId")                       
    if(type == -1):
        print("Vamos a ejecutar el sistema de guardados de videos")
        table_name = "observacion_aerea.captura_video"   
        SensorId = output_json.get("SensorID")  
        if(SensorId == None):
            SensorId = output_json.get("general", {}).get("SM", None)  
            if(SensorId != None):
                print("Estamos ante un video de tipo 2, en estos momentos esta comentada su funcionalidad")
                return
                #Normalizamos el json de general para que coincida con los tipo videos
                #output_json = generalizacionDatosMetadatos(output_json, output)



    if SensorId is None : 
        print("El recurso proporcionado no tiene id de sensor, no se guardarán metadatos.")
        try:
            upload_to_minio_path('minio_conn', 'cuarentena', 'sin_sensor' + '/', local_zip_path)
        except Exception as e:
            print(f"Error al subir el archivo a MinIO: {str(e)}")
        return

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

        

        try:
            if(type == 2): #Multiespectral
                date_time_str = output_json.get("DateTimeOriginal")
                timestamp_naive = datetime.strptime(date_time_str, "%Y:%m:%d %H:%M:%S")
            elif(type == -1): #Es video
                date_time_str = output_json.get("xmp:dateTimeOriginal")
                timestamp_naive = datetime.strptime(date_time_str, "%Y-%m-%dT%H:%M")
            else:
                date_time_str = output_json.get("DateTimeOriginal")
                date_time_original = datetime.strptime(date_time_str, "%Y:%m:%d %H:%M:%S%z")
                timestamp_naive = date_time_original.replace(tzinfo=None)

        except ValueError as ve:
            print(f"Error al parsear la fecha '{date_time_str}': {ve}")
            raise




        result = resultByType(type, message, output, output_json, query, SensorId, timestamp_naive, session)
        row = result.fetchone()

        print("Select realizado")
        
        #SE COMPRUEBA SI SE ACTUALIZA O CREA NUEVA
        if row:
            print(f"row: {row['fid']}")
            fid = row['fid']
            valid_time = row['valid_time']  
            
            if not (timestamp_naive in valid_time):  # Verifica si la fecha no está en el rango actual
                if timestamp_naive < valid_time.lower:  # Fecha antes del inicio del rango
                    diferencia = valid_time.lower - timestamp_naive
                    print(f"Fecha dada {timestamp_naive} está antes del inicio ({valid_time.lower}) en {diferencia} (h:m:s), se actualiza el inicio del rango.")

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
                        "new_start": timestamp_naive,
                        'fid' :  SensorId,   
                        'payload_id': output_json.get("PayloadSN"),
                        'multisim_id': output_json.get("MultisimSN"),
                        'ground_control_station_id': output_json.get("GroundControlStationSN"),
                        'pc_embarcado_id': output_json.get("PCEmbarcadoSN"),
                        'operator_name': output_json.get("OperatorName"),
                        'pilot_name': output_json.get("PilotName"),
                        'sensor': output_json.get("Model"),
                        'platform': output_json.get("AircraftNumberPlate")
                    })

                elif timestamp_naive > valid_time.upper:  # Fecha después del final del rango
                    diferencia = timestamp_naive - valid_time.upper
                    print(f"Fecha dada {timestamp_naive} está después del final ({valid_time.upper}) en {diferencia} (h:m:s), se actualiza el final del rango.")

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
                        "new_end": timestamp_naive,
                        'fid' :  SensorId,   
                        'payload_id': output_json.get("PayloadSN"),
                        'multisim_id': output_json.get("MultisimSN"),
                        'ground_control_station_id': output_json.get("GroundControlStationSN"),
                        'pc_embarcado_id': output_json.get("PCEmbarcadoSN"),
                        'operator_name': output_json.get("OperatorName"),
                        'pilot_name': output_json.get("PilotName"),
                        'sensor': output_json.get("Model"),
                        'platform': output_json.get("AircraftNumberPlate")
                    })
            else:
                print(f"Fecha dada {timestamp_naive} está dentro del rango {valid_time}, no se realiza actualización.")


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

            if(type == -1):
                # Generar los valores de inserción
                insert_values = {
                    'fid': int(SensorId),
                    'valid_time_start': timestamp_naive,
                    'valid_time_end': timestamp_naive + timedelta(minutes=1),  # Ajusta el rango inicial
                    'payload_id': output_json.get("PayloadSN"),
                    'multisim_id': output_json.get("MultisimSN"),
                    'ground_control_station_id': output_json.get("GroundControlStationSN"),
                    'pc_embarcado_id': output_json.get("PCEmbarcadoSN"),
                    'operator_name': output_json.get("OperatorName"),
                    'pilot_name': output_json.get("PilotName"),
                    'sensor': output_json.get("Model"),
                    'platform': output_json.get("AircraftNumberPlate")
                }
            if(type != -1):
                   insert_values = {
                    'fid': int(SensorId),
                    'valid_time_start': timestamp_naive,
                    'valid_time_end': timestamp_naive + timedelta(minutes=1),  # Ajusta el rango inicial
                    'payload_id': output_json.get("PayloadSN"),
                    'multisim_id': output_json.get("MultisimSN"),
                    'ground_control_station_id': output_json.get("GroundControlStationSN"),
                    'pc_embarcado_id': output_json.get("PCEmbarcadoSN"),
                    'operator_name': output_json.get("OperatorName"),
                    'pilot_name': output_json.get("PilotName"),
                    'sensor': output_json.get("Model"),
                    'platform': output_json.get("AircraftNumberPlate")
                }

            session.execute(insert_query, insert_values)
            session.commit()


        #INSERTAMOS EN OBSERVACION CAPTURA LA IMAGEN

        if (type == 0): #Es una visible
            table_name_observacion = "observacion_aerea.observation_captura_imagen_visible"
        if (type == 1): #Es una infrarroja
            table_name_observacion = "observacion_aerea.observation_captura_imagen_infrarroja"
        if (type == 2): #Es una multiespectral
            table_name_observacion = "observacion_aerea.observation_captura_imagen_multiespectral"
        if (type == -1): #Es un video
            table_name_observacion = "observacion_aerea.observation_captura_video"

        print("Insertamos en observación")
        
        outputt , outputcomment = parse_output_to_json(output)

        if(type != -1):
            insert_query = text(f"""
                INSERT INTO {table_name_observacion}
                ( shape, sampled_feature, procedure, result_time, phenomenon_time, imagen)
                VALUES ( :shape, :sampled_feature, :procedure, :result_time, 
                    :phenomenon_time, :imagen)
            """)

            shape = generar_shape_con_offsets(output_json)
            insert_values = {
                "shape": shape,
                "sampled_feature": '12306',
                "procedure": int(SensorId),
                "result_time":  timestamp_naive,
                "phenomenon_time": timestamp_naive,
                "imagen": outputt,
            }

        #ES UN VIDEO
        if(type == -1):

            insert_query = text(f"""
                INSERT INTO {table_name_observacion}
                ( procedure, sampled_feature, shape, result_time, phenomenon_time, video)
                VALUES ( :procedure, :sampled_feature, :shape, :result_time, 
                    tsrange(:valid_time_start, :valid_time_end, '[)'), :video)
            """)

            shape = generar_shape(output_json)
            out= json.loads(output)
            duration_in_seconds = duration_to_seconds(out.get("MediaDuration", 0))
            valid_time_end = timestamp_naive + timedelta(seconds=duration_in_seconds)
            output = json.loads(output)
            combined_json = {**output, **output_json}
   
            insert_values = {
                "shape": shape,
                "sampled_feature": '12306',
                "procedure": int(SensorId),
                "result_time":  timestamp_naive,
                "valid_time_start": timestamp_naive,
                "valid_time_end":  valid_time_end,
                "video": json.dumps(combined_json),
            }

        session.execute(insert_query, insert_values)
        session.commit()
        session.close()

    except Exception as e:
        print(f"Error al introducir la linea en observacion captura: {e}")
        raise 

    file_name = os.path.basename(message)
    mission_id = '12306'
    if mission_id != -1 :
        try:
            upload_to_minio_path('minio_conn', 'missions', mission_id + '/', local_zip_path)
        except Exception as e:
            print(f"Error al subir el archivo a MinIO: {str(e)}")
        return
    else : 
        try:
            upload_to_minio('minio_conn', 'missions', 'sin_mision_id' + '/' + file_name, local_zip_path)
        except Exception as e:
            print(f"Error al subir el archivo a MinIO: {str(e)}")
        return









def resultByType(type, message, output, output_json, query, SensorId, timestamp_naive, session):
    print(os.path.basename(message))
    if(type == -1): #ES UN VIDEO
            if(os.path.basename(message).startswith('EC')):
                print("Video tipo EC")
                result = session.execute(query, {
                    'fid' :  SensorId,   
                    'payload_id': output_json.get("PayloadSN", None),
                    'multisim_id': output_json.get("MultisimSN", None),
                    'ground_control_station_id': output_json.get("GroundControlStationSN", None),
                    'pc_embarcado_id': output_json.get("PCEmbarcadoSN", None),
                    'operator_name': output_json.get("OperatorName", None),
                    'pilot_name': output_json.get("PilotName", None),
                    'sensor': output_json.get("Camera Model Name"),
                    'platform': output_json.get("AircraftNumberPlate"),
                    'fecha_dada': timestamp_naive
                })
            if(os.path.basename(message).startswith('vid')):
                print("Video tipo vid")
                result = session.execute(query, {
                    'fid' :  SensorId,   
                    'payload_id': output_json.get("PayloadSN", None),
                    'multisim_id': output_json.get("MultisimSN", None),
                    'ground_control_station_id': output_json.get("GroundControlStationSN", None),
                    'pc_embarcado_id': output_json.get("PCEmbarcadoSN", None),
                    'operator_name': output_json.get("ON", None),
                    'pilot_name': output_json.get("PN", None),
                    'sensor': output_json.get("Camera Model Name"),
                    'platform': output_json.get("AP"),
                    'fecha_dada': timestamp_naive
                })
    else: #ES UNA IMAGEN
        result = session.execute(query, {
            'fid' :  SensorId,   
            'payload_id': output_json.get("PayloadSN"),
            'multisim_id': output_json.get("MultisimSN"),
            'ground_control_station_id': output_json.get("GroundControlStationSN"),
            'pc_embarcado_id': output_json.get("PCEmbarcadoSN"),
            'operator_name': output_json.get("OperatorName"),
            'pilot_name': output_json.get("PilotName"),
            'sensor': output_json.get("Model"),
            'platform': output_json.get("AircraftNumberPlate"),
            'fecha_dada': timestamp_naive
        })

    print("Valores para session.execute:")
    print(f"fid: {SensorId}")
    print(f"payload_id: {output_json.get('PayloadSN')}")
    print(f"multisim_id: {output_json.get('MultisimSN')}")
    print(f"ground_control_station_id: {output_json.get('GroundControlStationSN')}")
    print(f"pc_embarcado_id: {output_json.get('PCEmbarcadoSN')}")
    print(f"operator_name: {output_json.get('OperatorName')}")
    print(f"pilot_name: {output_json.get('PilotName')}")
    print(f"sensor: {output_json.get('Camera Model Name')}")
    print(f"platform: {output_json.get('AircraftNumberPlate')}")
    print(f"fecha_dada: {timestamp_naive}")

    return result





#--------------------------- METODOS AUXILIARES ------------------------------------------------
def generar_shape_con_offsets(data):

    gps_lat = data.get("GPSLatitude")
    gps_long = data.get("GPSLongitude")

    if(gps_lat is None or gps_long is None):
        print("No tenemos los campos de gps")
        return None
    
    # Convertir coordenadas de strings a valores numéricos
    lat_central = float(gps_lat.split()[0]) * (1 if gps_lat.split()[1] == "N" else -1)
    long_central = float(gps_long.split()[0]) * (1 if gps_long.split()[1] == "E" else -1)

    offsets = [
        data.get("OffsetCornerLongitudePoint1", 0),
        data.get("OffsetCornerLatitudePoint2", 0),
        data.get("OffsetCornerLongitudePoint2", 0),
        data.get("OffsetCornerLatitudePoint3", 0),
        data.get("OffsetCornerLongitudePoint3", 0),
        data.get("OffsetCornerLatitudePoint4", 0),
        data.get("OffsetCornerLongitudePoint4", 0),
        data.get("OffsetCornerLatitudePoint2", 0)  
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



def generar_shape(data):
    gps_lat_ref = data.get("GPS Latitude Ref")
    gps_lat = data.get("GPS Latitude")
    gps_long_ref = data.get("GPS Longitude Ref")
    gps_long = data.get("GPS Longitude")


    if gps_lat is None or gps_long is None:
        print("No tenemos los campos de GPS")
        return None

    if not isinstance(gps_lat, str) or not isinstance(gps_long, str):
        print(f"Formato inválido de coordenadas: {gps_lat}, {gps_long}")
        return None

    # Convertir coordenadas de DMS a formato decimal
    lat_central = dms_to_decimal(gps_lat, gps_lat_ref)
    long_central = dms_to_decimal(gps_long, gps_long_ref)

    # Validar que la conversión fue exitosa
    if lat_central is None or long_central is None:
        print("Error al convertir coordenadas a decimal")
        return None

    # Definir un pequeño desplazamiento (en grados decimales) para generar un área alrededor del punto
    offset = 0.0001  # Desplazamiento arbitrario para crear un área pequeña


    # Generar los vértices del polígono de un cuadrado alrededor del punto central
    vertices = [
        (long_central - offset, lat_central - offset),  # Suelo izquierdo
        (long_central - offset, lat_central + offset),  # Suelo derecho
        (long_central + offset, lat_central + offset),  # Arriba derecho
        (long_central + offset, lat_central - offset),  # Arriba izquierdo
    ]

    # Asegurar que el polígono esté cerrado (el primer punto al final)
    vertices.append(vertices[0])

    # Convertir a string en formato POLYGON
    vertices_str = ", ".join(f"{lon} {lat}" for lon, lat in vertices)
    shape = f"SRID=4326;POLYGON (({vertices_str}))"

    return shape

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

def generalizacionDatosMetadatos(output_json, output):
    output_load = json.loads(output)
    
    new_json = {
        "AircraftNumberPlate": output_json.get("general", {}).get("AP", ""),

        # "GPS Latitude Ref": "N", 
        # "GPS Latitude": "40° 57' 8.53''",  # Aquí, un ejemplo estático, pero podrías cambiar según datos disponibles.
        # "GPS Longitude Ref": "W",  # Lo mismo que para la latitud.
        # "GPS Longitude": "1° 17' 37.74''",  # Ejemplo estático.
        # "GPS Altitude": 1001,  # Este valor podría cambiar según los datos disponibles.
        # "GPS Speed": 57.0,  # Este también sería calculado o proporcionado dinámicamente.

        "xmp:dateTimeOriginal": datetime.now().strftime("%Y-%m-%dT%H:%M"),  # Fecha actual en formato ISO.
        "SensorID": output_json.get("general", {}).get("SM", None),  # Asumiendo que SM es el SensorID en tipo 2.
        "Make": "AXIS",  # Esto podría venir de otro lado si es dinámico.
        "Camera Model Name": "FA1080"  # Igualmente, esto puede cambiar si tienes información de la cámara.
    }
    return new_json


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