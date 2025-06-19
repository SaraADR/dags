import json
import tempfile
from botocore.client import Config
from jinja2 import Template
from airflow.operators.email import EmailOperator
import boto3
import os
import zipfile
from sqlalchemy import create_engine, Table, MetaData, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import re
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from airflow.hooks.base import BaseHook
import paramiko
from requests.auth import HTTPBasicAuth
from botocore.exceptions import ClientError
from moviepy import VideoFileClip, ImageClip

# Función para enviar notificaciones a la BD

def prepare_and_send_notification(conn_id, message, destination='ignis'):
    """
    Prepara y envía una notificación a la base de datos.
    """
    try:
        notification = json.dumps({
            "type": "job_created",
            "message": message,
            "destination": destination
        })
        
        query = """
        INSERT INTO public.notifications (destination, data)
        VALUES (:destination, :data)
        """
        params = {'destination': destination, 'data': notification}
        execute_query(conn_id, query, params)
        print(f"Notificación enviada a {destination}: {message}")
    except Exception as e:
        print(f"Error al enviar notificación: {str(e)}")
        raise

# Función para crear un archivo ZIP si existen los archivos requeridos por extensión y nombre de fichero
def crear_zip_si_existen(nombre_fichero, directorio, extensiones):
    # Extensiones necesarias
    #extensiones = ['.shp', '.prj', '.shx', '.dbf']
    archivos_encontrados = []
    
    # Buscar archivos con las extensiones requeridas
    for ext in extensiones:
        archivo = os.path.join(directorio, f"{nombre_fichero}{ext}")
        if os.path.exists(archivo):
            archivos_encontrados.append(archivo)
        else:
            print(f"Falta el archivo: {archivo}")
            raise RuntimeError(f"Falta el archivo: {archivo}")  # Salir si falta algún archivo
    
    # Crear el archivo ZIP
    zip_path = os.path.join(directorio, f"{nombre_fichero}.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for archivo in archivos_encontrados:
            zipf.write(archivo, os.path.basename(archivo))
            print(f"Archivo añadido al ZIP: {archivo}")
    
    print(f"ZIP creado exitosamente: {zip_path}")


# Configuración global del engine para reutilizarlo
def get_engine(connection_id: str = 'biobd'):
    """Crea y devuelve un engine reutilizable para la base de datos."""
    db_conn = BaseHook.get_connection(connection_id)
    db_name = db_conn.extra_dejson.get('database', 'postgres')
    connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/{db_name}"
    return create_engine(connection_string)

# Crear un engine global para que sea reutilizado
engine = get_engine()

# Función para obtener sesiones sin recrear el engine cada vez
def get_db_session():
    """Devuelve una nueva sesión de SQLAlchemy utilizando el engine global."""
    Session = sessionmaker(bind=engine)
    return Session()

# Función para enviar emails

def send_email_with_template(to, cc=None, bcc=None, subject=None, template_path=None, template_data=None, conn_id='test_mailing', attachments=None):
    """
    Envía un email utilizando una plantilla.
    """
    try:
        email_body = "Sin contenido"
        if template_path and template_data:
            with open(template_path) as file:
                template_str = file.read()
                jinja_template = Template(template_str)
            email_body = jinja_template.render(template_data)
        
        email_operator = EmailOperator(
            task_id='send_email_task',
            to=to,
            cc=cc,
            bcc=bcc,
            subject=subject,
            html_content=email_body,
            conn_id=conn_id,
            mime_subtype='related',
            files=attachments or []
        )
        email_operator.execute({})
        print("Email enviado correctamente")
    except Exception as e:
        print(f"Error al enviar el email: {str(e)}")
        raise



def list_files_in_minio_folder(s3_client, bucket_name, prefix):
    """
    Lista todos los archivos dentro de un prefijo (directorio) en MinIO.
    """

    print(bucket_name),
    print(prefix)
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in response:
            print(f"No se encontraron archivos en la carpeta: {prefix}")
            return []

        files = [content['Key'] for content in response['Contents']]
        return files

    except ClientError as e:
        print(f"Error al listar archivos en MinIO: {str(e)}")
        return []



def download_from_minio(s3_client, bucket_name, file_path_in_minio, local_directory, folder_prefix):
    """
    Función para descargar archivos o carpetas desde MinIO.
    """
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    local_file = os.path.join(local_directory, os.path.basename(file_path_in_minio))
    print(f"Descargando archivo desde MinIO: {file_path_in_minio} a {local_file}")
    
    relative_path = file_path_in_minio.replace('tmp/', '')
    print("RELATIVE PATH:" + relative_path)
    
    try:
        # Verificar si el archivo existe antes de intentar descargarlo
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

# Función para eliminar archivos antiguos de MinIO

def delete_old_files_from_minio(conn_id, bucket_name='temp', expiration_hours=24):
    """
    Elimina archivos antiguos de un bucket de MinIO.
    """
    try:
        connection = BaseHook.get_connection(conn_id)
        extra = json.loads(connection.extra)
        s3_client = boto3.client(
            's3',
            endpoint_url=extra['endpoint_url'],
            aws_access_key_id=extra['aws_access_key_id'],
            aws_secret_access_key=extra['aws_secret_access_key'],
            config=Config(signature_version='s3v4')
        )

        expiration_time = datetime.utcnow() - timedelta(hours=expiration_hours)
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' in objects:
            for obj in objects['Contents']:
                last_modified = obj['LastModified'].replace(tzinfo=None)
                if last_modified < expiration_time:
                    print(f"Eliminando {obj['Key']}...")
                    s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                    print(f"{obj['Key']} eliminado correctamente.")
        else:
            print("No se encontraron objetos en el bucket.")
    except Exception as e:
        print(f"Error al eliminar archivos en MinIO: {str(e)}")
        raise



# Función genérica para ejecutar SQL

def execute_query(conn_id, query, params=None):
    """
    Ejecuta una consulta SQL en la base de datos especificada.
    """
    try:
        db_conn = BaseHook.get_connection(conn_id)
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/{db_conn.schema or 'postgres'}"
        engine = create_engine(connection_string)

        with engine.connect() as connection:
            result = connection.execute(text(query), params or {})
            print("Consulta ejecutada correctamente")
            if(result is not None):
                return result.fetchall()
    except Exception as e:
        print(f"Error al ejecutar la consulta: {str(e)}")
        raise

# Función para enviar emails

def send_email(to, cc=None, bcc=None, subject=None, template_path=None, template_data=None, conn_id='test_mailing', attachments=None):
    """
    Envía un email utilizando Airflow.
    """
    try:
        # Renderizar la plantilla si existe
        if template_path and template_data:
            with open(template_path) as file:
                template_str = file.read()
                jinja_template = Template(template_str)
            email_body = jinja_template.render(template_data)
        elif template_data is not None and template_path is None:
             email_body = template_data
        else:      
            email_body = "Sin contenido"

        email_operator = EmailOperator(
            task_id='send_email_task',
            to=to,
            cc=cc,
            bcc=bcc,
            subject=subject,
            html_content=email_body,
            conn_id=conn_id,
            mime_subtype='related',
            files=attachments or []
        )
        email_operator.execute({})
        print("Email enviado correctamente")
    except Exception as e:
        print(f"Error al enviar el email: {str(e)}")
        raise


def get_minio_client():
    """Devuelve un cliente de MinIO reutilizable."""
    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)
    return boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )

def minio_api():
    """Devuelve el endpoint URL (host) de la API de MinIO desde la conexión de Airflow."""
    connection = BaseHook.get_connection('minio_api')
    extra = json.loads(connection.extra)
    return extra.get('endpoint_url')


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

def obtener_id_mision(fire_id):
    """
    Obtiene el mission_id (idMision) a partir del fire_id desde la tabla mss_mission_fire.
    """
    try:
        session = get_db_session()
        
        query = text("""
            SELECT mission_id 
            FROM missions.mss_mission_fire 
            WHERE fire_id = :fire_id;
        """)
        
        result = session.execute(query, {'fire_id': fire_id}).fetchone()

        if result:
            return result[0]
        else:
            print(f"No se encontró mission_id para fire_id: {fire_id}")
            return None

    except Exception as e:
        print(f"Error al obtener mission_id: {e}")
        return None


def duration_to_seconds(duration: str) -> int:
    """
    Convierte una duración en formato h:mm:ss (o m:ss) a segundos.

    :param duration: Duración como cadena, por ejemplo "0:00:30" o "1:15:30".
    :return: Duración total en segundos.
    """
    # Expresión regular para extraer horas, minutos y segundos
    match = re.match(r"(?:(\d+):)?(\d{1,2}):(\d{2})", duration)
    
    if match:
        # Si se encuentra el formato, obtenemos horas, minutos y segundos
        hours = int(match.group(1) or 0)  # Horas, por defecto 0 si no se encuentran
        minutes = int(match.group(2))     # Minutos
        seconds = int(match.group(3))     # Segundos

        # Convertimos todo a segundos
        total_seconds = hours * 3600 + minutes * 60 + seconds
        return total_seconds
    else:
        raise ValueError(f"Formato de duración inválido: {duration}")
    

# Función para actualizar estados

def update_job_status(job_id, status, output_data = None, execution_date=None):
    try:
        # Conexión a la base de datos usando las credenciales de Airflow
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        metadata = MetaData(bind=engine)

        # Tabla de trabajos
        jobs = Table('jobs', metadata, schema='public', autoload_with=engine)

        # Actualizar el estado del trabajo
        with engine.connect() as connection:
            if(execution_date is not None):
                update_stmt = jobs.update().where(jobs.c.id == job_id).values(status=status, execution_date=execution_date, output_data=output_data)
            else:
                update_stmt = jobs.update().where(jobs.c.id == job_id).values(status=status, output_data=output_data)
            connection.execute(update_stmt)
            print(f"Job ID {job_id} status updated to {status}")
    except Exception as e:
        print(f"Error al actualizar el estado del trabajo: {e}")
        raise

def throw_job_error(job_id, e):
    obj_json = {
        "errorMessage": f"Error en el proceso: {str(e)}"
    }
    update_job_status(job_id, 'ERROR', json.dumps(obj_json, ensure_ascii = False))

# ============================
# Función para subir archivos a MinIO
# ============================
def upload_to_minio(conn_id, bucket_name, file_key, file_content):
    """
    Sube un archivo a MinIO.
    """
    try:
        connection = BaseHook.get_connection(conn_id)
        extra = json.loads(connection.extra)
        s3_client = boto3.client(
            's3',
            endpoint_url=extra['endpoint_url'],
            aws_access_key_id=extra['aws_access_key_id'],
            aws_secret_access_key=extra['aws_secret_access_key'],
            config=Config(signature_version='s3v4')
        )

        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=file_content
        )
        print(f"Archivo {file_key} subido correctamente a MinIO.")
    except Exception as e:
        print(f"Error al subir el archivo a MinIO: {str(e)}")
        raise


def get_geoserver_connection(conn_id='geoserver_connection'):
    """
    Devuelve la información de conexión a GeoServer a partir del conn_id.
    """
    try:
        conn = BaseHook.get_connection(conn_id)
        base_url = conn.host.rstrip('/')
        username = conn.login
        password = conn.password
        auth = HTTPBasicAuth(username, password)

        print(f"Conexión a GeoServer obtenida de {conn_id}")
        return base_url, auth
    except Exception as e:
        print(f"Error al obtener conexión a GeoServer: {str(e)}")
        raise

def upload_to_minio_path(conn_id, bucket_name, destination_prefix, local_file):
    """
    Función para subir un archivo desde la máquina local a MinIO.
    """
    try:
        connection = BaseHook.get_connection(conn_id)
        extra = json.loads(connection.extra)
        s3_client = boto3.client(
            's3',
            endpoint_url=extra['endpoint_url'],
            aws_access_key_id=extra['aws_access_key_id'],
            aws_secret_access_key=extra['aws_secret_access_key'],
            config=Config(signature_version='s3v4')
        )
    
        destination_file_path = os.path.join(destination_prefix, os.path.basename(local_file))
    
        # Subir el archivo al bucket de destino
        print(f"Subiendo archivo a MinIO: {destination_file_path}")
        s3_client.upload_file(local_file, bucket_name, destination_file_path)
        print(f"Archivo subido correctamente a: {destination_file_path}")
        
    except Exception as e:
        print(f"Error al subir el archivo a MinIO: {str(e)}")
        raise

# Función para listar archivos en MinIO

def list_files_in_minio(conn_id, bucket_name, prefix):
    """
    Lista archivos en un bucket/prefijo específico en MinIO.
    """
    try:
        connection = BaseHook.get_connection(conn_id)
        extra = json.loads(connection.extra)
        s3_client = boto3.client(
            's3',
            endpoint_url=extra['endpoint_url'],
            aws_access_key_id=extra['aws_access_key_id'],
            aws_secret_access_key=extra['aws_secret_access_key'],
            config=Config(signature_version='s3v4')
        )

        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            files = [content['Key'] for content in response['Contents']]
            return files
        else:
            print(f"No se encontraron archivos en {prefix}")
            return []

    except Exception as e:
        print(f"Error al listar archivos en MinIO: {str(e)}")
        raise


# Función para convertir coordenadas GeoJSON a WKT
def geojson_to_wkt(geojson):
    """
    Convierte un objeto GeoJSON a WKT.
    """
    try:
        x = geojson['x']
        y = geojson['y']
        z = geojson.get('z', 0)  # Default Z coordinate
        return f"POINT Z({x} {y} {z})"
    except KeyError as e:
        print(f"Error al convertir GeoJSON a WKT: {str(e)}")
        raise


def dms_to_decimal(dms_value, ref):
    """
    Convierte una coordenada DMS (grados, minutos, segundos) a formato decimal.
    La referencia (N/S para latitud, E/W para longitud) indica el signo.
    """
    try:
        # Reemplazar comillas dobles y limpiar espacios
        dms_value = dms_value.replace("''", "").strip()

        print(f"Procesando DMS: {dms_value}")

        # Separar grados
        dms_parts = dms_value.split('°')
        if len(dms_parts) != 2:
            raise ValueError(f"Formato DMS inválido: {dms_value}")

        degrees = dms_parts[0].strip()
        minutes_seconds = dms_parts[1].split("'")
        # Separar minutos y segundos
        if len(minutes_seconds) != 2:
            raise ValueError(f"Formato minutos/segundos inválido: {dms_value}")

        minutes = minutes_seconds[0].strip()
        seconds = minutes_seconds[1].strip()
        # Convertir a decimal
        decimal = float(degrees) + float(minutes) / 60 + float(seconds) / 3600

        # Ajustar el signo según la referencia (N/S/E/W)
        if ref in ["S", "W"]:
            decimal = -decimal

        return decimal

    except ValueError as e:
        print(f"Error al convertir DMS a decimal: {e}")
        return None
    except Exception as e:
        print(f"Error inesperado: {e}")
        return None
    




#Funcion que convierte un string en json
def parse_output_to_json(output):
    """
    Toma el output del comando docker como una cadena de texto y lo convierte en un diccionario JSON.
    """
    metadata = {}
    comment_json = None

    # Expresión regular para capturar pares clave-valor separados por ":"
    pattern = r"^(.*?):\s*(.*)$"
    for line in output.splitlines():
        match = re.match(pattern, line)
        if match:
            key = match.group(1).strip()
            key = key.strip()
            value = match.group(2).strip()
            if key == "Comment":
                try:
                    # Intentar cargar el valor como JSON
                    comment_json = json.loads(value.strip("'"))
                except json.JSONDecodeError:
                    print(f"Error al procesar el JSON dentro de 'comment': {value}")
                    comment_json = None
            else:
                metadata[key] = value

    metadata_json = json.dumps(metadata, ensure_ascii=False, indent=4)
    comment_json_formatted = json.dumps(comment_json, ensure_ascii=False, indent=4) if comment_json else {}

    
    return metadata_json, comment_json_formatted


#Funcion para pasar una string de segundos a segundos
def duration_to_seconds(duration_str):
    h, m, s = map(int, duration_str.split(":"))
    return timedelta(hours=h, minutes=m, seconds=s).total_seconds()


def delete_file_sftp(url):

    filename = os.path.basename(url)
    try:
        conn = BaseHook.get_connection('SFTP')
        host = conn.host
        port = conn.port 
        username = conn.login
        password = conn.password


        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        sftp.remove(filename)
        print(f"Archivo '{filename}' eliminado exitosamente.")

        # Cerrar conexiones
        sftp.close()
        transport.close()

    except Exception as e:
        print(f"Error al eliminar el archivo: {e}")


def upload_logs_to_s3(context):
    try:
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        execution_date = context['ts_nodash']
        try_number = context['task_instance'].try_number

        log_file_path = f"/opt/airflow/logs/{dag_id}/{task_id}/{execution_date}/attempt={try_number}.log"
        
        print(f"🪵 Subiendo logs desde: {log_file_path}")
        
        with open(log_file_path, "r") as log_file:
            logs = log_file.read()
        
        print(f"📄 Logs:\n{logs[:500]}...")  # solo los primeros 500 caracteres
    except Exception as e:
        print(f"❌ Error al leer logs: {e}")

    marker_path = f"/tmp/on_success_marker_{dag_id}_{task_id}_{execution_date}.txt"
    try:
        with open(marker_path, "w") as f:
            f.write("✅ Callback ejecutado\n")

        print(f"🪵 Callback ejecutado correctamente, se creó: {marker_path}")
    except Exception as e:
        print(f"❌ Error en el callback: {e}")



def generate_thumbnail(image, image_name):
    """Genera miniaturas a partir de un fichero"""
    if not image:
        return

    temp_dir = tempfile.mkdtemp()
    original_ext = os.path.splitext(image_name)[-1].lower()
    image_path = os.path.join(temp_dir, f"original{original_ext}")
    thumbnail_path = os.path.join(temp_dir, f"{os.path.splitext(image_name)[0]}_thumb.jpg")
    thumbnail_name = f"{os.path.splitext(image_name)[0]}_thumb.jpg"

    try:
        with open(image_path, "wb") as f:
            f.write(image)


        clip = ImageClip(image_path)
        clip = clip.resize(height=256)  
        clip.save_frame(thumbnail_path)

        with open(thumbnail_path, "rb") as f:
            thumbnail_content = f.read()

        return thumbnail_content, thumbnail_name
    
    except Exception as e:
        print(f"Error procesando {image_name}: {e}")
        return None, None
    finally:
        # Limpieza de archivos temporales
        os.remove(image_path) if os.path.exists(image_path) else None
        os.remove(thumbnail_path) if os.path.exists(thumbnail_path) else None
        os.rmdir(temp_dir)

