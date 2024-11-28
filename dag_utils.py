import json
import boto3
from botocore.client import Config
from jinja2 import Template
from airflow.operators.email import EmailOperator
from sqlalchemy import create_engine, text
from airflow.hooks.base_hook import BaseHook
import json
import boto3
import os
from botocore.client import Config
from jinja2 import Template
from airflow.operators.email import EmailOperator
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import re


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


# Función para ejecutar consultas SQL

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
            return result.fetchall()
    except Exception as e:
        print(f"Error al ejecutar la consulta: {str(e)}")
        raise



# Función para actualizar estados

def update_job_status(conn_id, job_id, status):
    """
    Actualiza el estado de un trabajo en la base de datos.
    """
    query = """
    UPDATE public.jobs
    SET status = :status
    WHERE id = :job_id
    """
    params = {'job_id': job_id, 'status': status}
    execute_query(conn_id, query, params)
    print(f"Estado del job {job_id} actualizado a {status}")


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

        print(f"Grados: {degrees}")
        print(f"Minutos y segundos: {minutes_seconds}")

        # Separar minutos y segundos
        if len(minutes_seconds) != 2:
            raise ValueError(f"Formato minutos/segundos inválido: {dms_value}")

        minutes = minutes_seconds[0].strip()
        seconds = minutes_seconds[1].strip()

        print(f"Minutos: {minutes}, Segundos: {seconds}")

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
            if key == "comment":
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
