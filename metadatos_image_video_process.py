import json
import os
import shutil
import uuid
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
from dag_utils import dms_to_decimal, duration_to_seconds, get_minio_client, minio_api, parse_output_to_json, upload_to_minio, upload_to_minio_path, send_email
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from dag_utils import get_db_session, delete_file_sftp
from airflow.models import Variable
from confluent_kafka import Consumer, KafkaException
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import get_current_context

KAFKA_RAW_MESSAGE_PREFIX = "Mensaje crudo:"

mensaje_final = {
    "key": "ImagenMetadatos",
    "value": {
        "RutaImagen": None,
        "IdDeTabla": None,
        "TablaGuardada": None
    }
}

def poll_kafka_messages(**kwargs):
    conf = {
        'bootstrap.servers': '10.96.180.179:9092',
        'group.id': '1',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }

    consumer = Consumer(conf)
    consumer.subscribe(['metadata11'])
    messages = []

    try:
        while True:
            msg = consumer.poll(timeout=10.0)
            if msg is None:
                print("No hay más mensajes que leer en el topic")
                break  
            if msg.error():
                raise KafkaException(msg.error())
            else:
                print(f"{KAFKA_RAW_MESSAGE_PREFIX} {msg}")
                
                msg_value = msg.value().decode('utf-8')
                print("Mensaje procesado: ", msg_value)
                messages.append(msg_value)

        
        if messages:
            print(f"Total messages received: {len(messages)}")
            consumer.commit()

            s3_client = get_minio_client()

            # Nombre del bucket donde está almacenado el archivo/carpeta
            bucket_name = 'tmp'
            folder_prefix = 'metadatos/'
            local_directory = 'tmp'  
            for msg_value in messages:
                file_path_in_minio =  msg_value.replace('tmp/', '')
                try:
                    local_zip_path = download_from_minio(s3_client, bucket_name, file_path_in_minio, local_directory, folder_prefix)
                    print(local_zip_path)
                    process_zip_file(local_zip_path, file_path_in_minio, msg_value,  **kwargs)
                    delete_file_sftp(msg_value)
                except Exception as e:
                    print(f"Error al descargar desde MinIO: {e}")
                    raise 

    finally:
        consumer.close()


#HACE LECTURA DE METADATOS Y CONTROLA EL TIPO ENCONTRADO
def process_zip_file(local_zip_path, file_path, message, **kwargs):

    if local_zip_path is None:
        print(f"No se pudo descargar el archivo desde MinIO: {local_zip_path}")
        return
    
    name_short = os.path.basename(file_path)
    file_name = 'tmp/' + name_short
    print(f"Ejecutando proceso de docker con el file {file_name}")
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')

    try:
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()


            shared_volume_path = f"/home/admin3/exiftool/exiftool/images/{name_short}"

            sftp.put(file_name, shared_volume_path)
            print(f"Copied {file_name} to {shared_volume_path}")

            #Comprobamos el dato Version para ver si esta disponible
            docker_command = (
                f'cd /home/admin3/exiftool/exiftool && '
                f'docker run --rm -v /home/admin3/exiftool/exiftool:/images '
                f'--name exiftool-container-{name_short.replace(".", "-")} '
                f'exiftool-image -u -s -b -Exif_0xd059 /images/images/{name_short}'
            )
            stdin, stdout, stderr = ssh_client.exec_command(docker_command , get_pty=True)
            output = stdout.read().decode().strip()
            print(f"Versión del metadato para la imagen {file_name}:")
            print(output)

            version_map = {
                "3.0.0": "/images/images/example3.0.0_20250206.txt",
                "1.0.5": "/images/images/example1.0.5.txt",
                "1.0.8": "/images/images/example1.0.8.txt",
                "1.1.0": "/images/images/example1.1.0.txt"
            }
            # Lógica para determinar la versión correcta
            if not output:  # Si el metadato no está presente
                version = "/images/images/example1.0.5.txt"
            elif output in version_map:  # Si el metadato coincide con una versión conocida
                version = version_map[output]
            else:  # Si el metadato está presente pero no es ninguna versión conocida
                version = "/images/images/example3.0.0_20250206.txt"
                textoEmail = (f"Se ha encontrado la version {output} que no corresponde con ninguna de las que tenemos")
                send_email('ejemplo@gmail.com', None, None, "Versión encontrada no reconocida", None, textoEmail)



            print(f"La version seleccionada es: {version}")

            # Clean up Docker container after each run
            cleanup_command = f'docker rm exiftool-container-{name_short.replace(".", "-")}'
            ssh_client.exec_command(cleanup_command)

            # Execute Docker command for each file
            docker_command = (
                f'cd /home/admin3/exiftool/exiftool && '
                f'docker run --rm -v /home/admin3/exiftool/exiftool:/images '
                f'--name exiftool-container-{name_short.replace(".", "-")} '
                f'exiftool-image -config {version} -u -s /images/images/{name_short}'
            )
            print(docker_command)

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
    if idRafaga not in ['0', '', None]:
        is_rafaga(output, output_json, version,ruta_imagen=file_path, **kwargs)


    #SON VIDEOS
    elif message.endswith(".mp4"):
        comments = json.loads(comment_json)
        is_visible_or_ter(message, local_zip_path, output_json_noload, comments, -1, version)
    elif message.endswith(".ts"):
        process_ts_job(output, message, local_zip_path)
        print(f"Archivo {message} procesado con éxito.")
    

    #SON IMAGENES
    elif "-vis" in message:
        #Es imagen visible
        is_visible_or_ter(message,local_zip_path, output_json_noload, output_json, 0, version)
    elif "-ter" in message:
        # Es termodinamica
        is_visible_or_ter(message,local_zip_path,output_json_noload,output_json, 1, version)
    elif "-mul" in message:
        # Es multiespectral
        is_visible_or_ter(message,local_zip_path,output_json_noload,output_json, 2, version)
    else:
        if output_json.get("sensor_id") == 1:
             is_visible_or_ter(message,local_zip_path,output_json_noload,output_json, 0, version)
        elif output_json.get("sensor_id") == 2:
             is_visible_or_ter(message,local_zip_path,output_json_noload,output_json, 1, version)
        else:
            is_visible_or_ter(message,local_zip_path, output_json_noload,output_json, 0, version)
            print("No se reconoce el tipo de imagen o video aportado")
        return
    
    try:
        if os.path.exists(local_zip_path):
            os.remove(local_zip_path)
            print(f"Archivo local eliminado: {local_zip_path}")
    except Exception as e:
        print(f"Error eliminando archivo local: {e}")
        
    return










def process_ts_job(output, message, local_zip_path):
    """
    Procesa un archivo .ts y registra un trabajo en la base de datos, utilizando el nombre del archivo como resource_id.
    """
    print(f"Procesando archivo .ts: {message}")

    try:
        # Extraer el nombre del archivo sin la extensión para usarlo como resource_id
        resource_id = message.split("/")[-1].replace(".ts", "")  # Obtiene el nombre del archivo (sin la extensión .ts)
        print(f"Resource ID asignado: {resource_id}")
        
        time_now = datetime.now(timezone.utc)

        # Conectar a la base de datos
        session = get_db_session()
        engine = session.get_bind()

        # Crear JSON de entrada con el nombre del archivo como resource_id
        data_json = f'{{"resource_id": "{resource_id}"}}'

        # Insertar trabajo inicial en la base de datos
        query_insert = text("""
            INSERT INTO public.jobs
            (job, input_data, application_date, status, from_user)
            VALUES (:job_name, :data, :date, 'QUEUED', :from_user)
            RETURNING id;
        """)
        result = session.execute(query_insert, {
            'job_name': "convert-ts-to-mp4",
            'data': data_json,
            'date': time_now,
            'from_user': "Francisco José Blanco Garza"
        })
        job_id = result.fetchone()[0]
        session.commit()

        print(f"Notificación enviada a jobs para archivo: {message}")
        print(f"Job creado con Id {job_id} para el archivo {resource_id}.")
    
    except Exception as e:
        session.rollback()
        print(f"Error durante el procesamiento o inserción del trabajo: {str(e)}")
    finally:
        session.close()




##--------------------------- PROCEDIMIENTO DE RAFAGAS ------------------------------------------------
def is_rafaga(output, output_json, version, ruta_imagen=None, **kwargs):
    conf_dict = {
        'output': output,
        'output_json': output_json,
        'version': version,
        'RutaImagen': ruta_imagen  # <-- AÑADIDO: pasa la ruta aquí explícitamente
    }

    TriggerDagRunOperator(
        task_id=f"trigger_rafagas_{uuid.uuid4()}",
        trigger_dag_id='process_rafagas_and_metadatos',
        conf=conf_dict,
        execution_date=datetime.now().replace(tzinfo=timezone.utc),
        dag=kwargs['dag']
    ).execute(context=kwargs)


##--------------------------- PROCEDIMIENTO DE IMAGENES Y VIDEOS ---------------------------------------
def is_visible_or_ter(message, local_zip_path, output, output_json, type, versionConfigExiftool, **kwargs):

    #Recogemos el sensor
    sensor_key = "SensorID" if type != 2 else "SensorId"
    SensorId = output_json.get(sensor_key)

    #Seleccionamos la tabla adecuada
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
        print("Vamos a ejecutar el sistema de guardados de videos")
        table_name = "observacion_aerea.captura_video"   
        if(SensorId == None):
            SensorId = output_json.get("general", {}).get("SM", None)  
            if(SensorId != None):
                print("Estamos ante un video de tipo 2, en estos momentos esta comentada su funcionalidad")
                trigger_video_dag(output_json, message, versionConfigExiftool, **kwargs)
                print("Se ha activado el trigger de video tipo 2")
                
                return
                #Normalizamos el json de general para que coincida con los tipo videos
                #output_json = generalizacionDatosMetadatos(output_json, output)



    if SensorId is None : 
        key = f"{uuid.uuid4()}"
        print("El recurso proporcionado no tiene id de sensor, no se guardarán metadatos.")
        try:
            set_mensaje_final('cuarentena/' + 'sin_sensor' + '/' + str(key), None, None)
            upload_to_minio_path('minio_conn', 'cuarentena', 'sin_sensor' + '/' + str(key), local_zip_path)
        except Exception as e:
            print(f"Error al subir el archivo a MinIO: {str(e)}")
        return
    



    # Buscar los metadatos en captura
    try:
        session = get_db_session()
        engine = session.get_bind()

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

        
        #Buscamos la fecha del recurso
        try:
            if(type != -1): #Multiespectral
                date_time_str = output_json.get("DateTimeOriginal")
                try:
                    date_time_original = datetime.strptime(date_time_str, "%Y:%m:%d %H:%M:%S%z")
                except ValueError:
                    try:
                        date_time_original = datetime.strptime(date_time_str, "%Y:%m:%d %H:%M:%S")
                    except ValueError:
                        date_time_original = datetime.strptime(date_time_str, "%Y:%m:%d %H:%M:%S.%fZ")
                timestamp_naive = date_time_original.replace(tzinfo=None)
                
            elif(type == -1): #Video
                date_time_str = output_json.get("xmp:dateTimeOriginal")
                timestamp_naive = datetime.strptime(date_time_str, "%Y-%m-%dT%H:%M")

        except ValueError as ve:
            print(f"Error al parsear la fecha '{date_time_str}': {ve}")
            raise


        # SE HACE BUSQUEDA POR FECHA Y TIPO
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
                    update_column, update_value = "lower(valid_time)", ":new_start"
                    query_param = {"new_start": timestamp_naive}

                elif timestamp_naive > valid_time.upper: # Fecha despues del inicio del rango
                    diferencia = timestamp_naive - valid_time.upper
                    print(f"Fecha dada {timestamp_naive} está después del final ({valid_time.upper}) en {diferencia} (h:m:s), se actualiza el final del rango.")
                    update_column, update_value = "upper(valid_time)", ":new_end"
                    query_param = {"new_end": timestamp_naive}
                else:
                    # Si la fecha está dentro del rango, no se necesita actualización
                    update_column, update_value, query_param = None, None, None


                if update_column:
                    # Actualizar el inicio del rango
                    update_query = text(f"""
                        UPDATE {table_name}
                        SET valid_time = tsrange({update_value}, {update_column}, '[)')  
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
                    query_param.update({
                        "fid": SensorId,
                        "payload_id": output_json.get("PayloadSN"),
                        "multisim_id": output_json.get("MultisimSN"),
                        "ground_control_station_id": output_json.get("GroundControlStationSN"),
                        "pc_embarcado_id": output_json.get("PCEmbarcadoSN"),
                        "operator_name": output_json.get("OperatorName"),
                        "pilot_name": output_json.get("PilotName"),
                        "sensor": output_json.get("Model"),
                        "platform": output_json.get("AircraftNumberPlate")
                    })
                    session.execute(update_query, query_param)
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


            sensor_key = "Camera Model Name" if type == -1 else "Model"

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
                'sensor': output_json.get(sensor_key),
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
        set_mensaje_final(None, None, table_name_observacion)
        
        outputt , outputcomment = parse_output_to_json(output)

        mensaje_final_str = Variable.get("mensaje_final", "{}")  # obtiene string JSON
        mensaje_final = json.loads(mensaje_final_str)             # convierte a dict
        ruta_imagen = mensaje_final.get("value", {}).get("RutaImagen", "")
    
        file_name = output_json.get("FileName", "")
        base_name = os.path.splitext(os.path.basename(file_name))[0]
        thumbnail_key = f"thumbs/{base_name}_thumb.jpg"

        #Le añadimos la version
        metadata_dict = json.loads(outputt)
        metadata_dict["ReadedFromVersion"] = versionConfigExiftool
        metadata_dict["original"] = f"{minio_api()}/{ruta_imagen}"
        metadata_dict["url"] = f"{minio_api()}/tmp/{thumbnail_key}"
        print(metadata_dict)


        outputt = json.dumps(metadata_dict, ensure_ascii=False, indent=4)


        #ES UNA IMAGEN
        if(type != -1):
            insert_query = text(f"""
                INSERT INTO {table_name_observacion}
                ( shape, sampled_feature, procedure, result_time, phenomenon_time, imagen)
                VALUES ( :shape, :sampled_feature, :procedure, :result_time, 
                    :phenomenon_time, :imagen)
                RETURNING fid;
            """)

            shape = generar_shape_con_offsets(output_json)
            insert_values = {
                "shape": shape,
                "sampled_feature": output_json.get("MissionID"),
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
                RETURNING fid;
            """)

            shape = generar_shape(output_json)
            out= json.loads(output)
            duration_in_seconds = duration_to_seconds(out.get("MediaDuration", 0))
            valid_time_end = timestamp_naive + timedelta(seconds=duration_in_seconds)
            output = json.loads(output)
            combined_json = {**output, **output_json}
   
            insert_values = {
                "shape": shape,
                "sampled_feature": output_json.get("MissionID", None),
                "procedure": int(SensorId),
                "result_time":  timestamp_naive,
                "valid_time_start": timestamp_naive,
                "valid_time_end":  valid_time_end,
                "video": json.dumps(combined_json),
            }

        result = session.execute(insert_query, insert_values)
        new_id = result.scalar()
        set_mensaje_final(None, new_id, None)
        session.commit()
        session.close()

    except Exception as e:
        print(f"Error al introducir la linea en observacion captura: {e}")
        raise 

    file_name = os.path.basename(message)
    mission_id = output_json.get("MissionID", -1)
    if mission_id != -1 :
        try:
            key = f"{uuid.uuid4()}"
            upload_to_minio_path('minio_conn', 'missions', mission_id + '/' + str(key) , local_zip_path)
            set_mensaje_final('missions/' + mission_id + '/' + str(key) + '/' + file_name, None, None)
        except Exception as e:
            print(f"Error al subir el archivo a MinIO: {str(e)}")
        return
    else : 
        try:
            key = f"{uuid.uuid4()}"
            upload_to_minio('minio_conn', 'missions', 'sin_mision_id' + '/' + str(key) + '/' + file_name, local_zip_path)
            set_mensaje_final('missions/' + 'sin_mision_id' + '/' + str(key) + '/' + file_name, None, None)
        except Exception as e:
            print(f"Error al subir el archivo a MinIO: {str(e)}")
        return


def resultByType(type, message, output, output_json, query, SensorId, timestamp_naive, session):


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

def trigger_video_dag(output_json, ruta_video, version, **kwargs):
    conf_dict = {
        'output_json': output_json,
        'RutaVideo': ruta_video,
        'version': version
    }
    context = get_current_context()
    dag = context['dag']

    TriggerDagRunOperator(
        task_id=f"trigger_video_process_{uuid.uuid4()}",
        trigger_dag_id='process_video_and_metadatos',
        conf=conf_dict,
        execution_date=datetime.now().replace(tzinfo=timezone.utc),
        dag=dag
    ).execute(context=context)


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
    Función para descargar archivos desde MinIO usando chunks para reducir uso de memoria.
    """
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    local_file = os.path.join(local_directory, os.path.basename(file_path_in_minio))
    print(f"Descargando archivo desde MinIO: {file_path_in_minio} a {local_file}")
    relative_path = file_path_in_minio.replace('/temp/', '')

    try:
        # Obtener objeto sin cargar contenido completo en memoria
        response = s3_client.get_object(Bucket=bucket_name, Key=relative_path)
        
        # Escribir en chunks de 8KB para minimizar uso de memoria
        CHUNK_SIZE = 8192  # 8KB chunks
        
        with open(local_file, 'wb') as f:
            while True:
                chunk = response['Body'].read(CHUNK_SIZE)
                if not chunk:
                    break
                f.write(chunk)

        print(f"Archivo descargado correctamente: {local_file}")
        return local_file
        
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Error 404: El archivo no fue encontrado en MinIO: {file_path_in_minio}")
        else:
            print(f"Error en el proceso: {str(e)}")
        return None

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


def set_mensaje_final(ruta_imagen, id_de_tabla, tabla_guardada):
    global mensaje_final
    if ruta_imagen:
        mensaje_final["value"]["RutaImagen"] = ruta_imagen
    if id_de_tabla:    
        mensaje_final["value"]["IdDeTabla"] = id_de_tabla
    if tabla_guardada:
        mensaje_final["value"]["TablaGuardada"] = tabla_guardada
    print("Actualización de mensaje final")
    Variable.set("mensaje_final", json.dumps(mensaje_final))
    print(mensaje_final)



def my_producer_function():
    global mensaje_final
    mensaje_final = json.loads(Variable.get("mensaje_final", "{}"))
    print(f"Se envía el mensaje al topic {mensaje_final}")

    if not mensaje_final:
        print("No se envia ningun mensaje pues no se ha proporcionado nueva información")
        return [] 
    
    valorFinal = json.dumps(mensaje_final["value"])  
    print(valorFinal)  
    return [("ImagenMetadatos", valorFinal)] 

def delete_variable_global(**kwargs):
    global mensaje_final
    Variable.set("mensaje_final", {})




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
    'metadatos_image_video_process',
    default_args=default_args,
    description='DAG procesa metadatos',
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=5,
)

poll_task = PythonOperator(
        task_id='poll_kafka',
        python_callable=poll_kafka_messages,
        provide_context=True,
        dag=dag
)

produce_task = ProduceToTopicOperator(
    task_id="produce_to_kafka",
    topic="thumbs",
    kafka_config_id="kafka_connection", 
    dag=dag,
    producer_function=my_producer_function
)

delete_global_task = PythonOperator(
    task_id='delete_global_task',
    python_callable=delete_variable_global,
    provide_context=True,
    dag=dag,
)

poll_task >> produce_task >> delete_global_task