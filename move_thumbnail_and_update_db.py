import json
import os
import time
import boto3
from botocore.client import Config
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import text
from dag_utils import get_db_session
from airflow.operators.python_operator import PythonOperator


# Función para cargar archivos pendientes desde MinIO
def load_pending_files_from_minio(s3_client, bucket_name, key):
    """Carga la lista de miniaturas pendientes desde MinIO."""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except s3_client.exceptions.NoSuchKey:
        return []
    except Exception as e:
        print(f"[ERROR] Error al cargar miniaturas pendientes: {e}")
        return []

# Función para guardar archivos pendientes en MinIO
def save_pending_files_to_minio(s3_client, bucket_name, key, pending_files):
    """Guarda la lista de miniaturas pendientes en MinIO."""
    try:
        json_data = json.dumps(pending_files, indent=4)
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json_data)
    except Exception as e:
        print(f"[ERROR] Error al guardar miniaturas pendientes: {e}")

# Procesa miniaturas pendientes antes de Kafka
def process_pending_thumbnails():
    """Revisa `pending_thumbnails.json` e intenta procesar las miniaturas pendientes."""
    print("[INFO] Revisando miniaturas pendientes...")

    connection = BaseHook.get_connection('minio_conn')
    extra = json.loads(connection.extra)
    s3_client = boto3.client(
        's3',
        endpoint_url=extra['endpoint_url'],
        aws_access_key_id=extra['aws_access_key_id'],
        aws_secret_access_key=extra['aws_secret_access_key'],
        config=Config(signature_version='s3v4')
    )
    
    bucket_name = "tmp"
    pending_file_key = "pending_thumbnails.json"

    # Cargar miniaturas pendientes
    pending_thumbnails = load_pending_files_from_minio(s3_client, bucket_name, pending_file_key)
    if not pending_thumbnails:
        print("[INFO] No hay miniaturas pendientes.")
        return

    print(f"[INFO] Miniaturas pendientes detectadas: {len(pending_thumbnails)}")

    updated_pending_thumbnails = []

    for thumbnail in pending_thumbnails:
        ruta_imagen_original = thumbnail["RutaImagen"]
        id_tabla = thumbnail["IdDeTabla"]
        tabla_guardada = thumbnail["TablaGuardada"]

        nombre_archivo = os.path.basename(ruta_imagen_original)
        thumbnail_key = f"thumbs/{nombre_archivo}"
        nueva_ruta_thumbnail = f"{os.path.dirname(ruta_imagen_original)}/{nombre_archivo}"

        print(f"[INFO] Verificando existencia de {thumbnail_key} en MinIO...")

        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=thumbnail_key)
        if 'Contents' in response:
            print(f"[INFO] Miniatura encontrada en MinIO. Moviendo archivo...")

            # Mover la miniatura
            copy_source = {"Bucket": bucket_name, "Key": thumbnail_key}
            s3_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=nueva_ruta_thumbnail)
            s3_client.delete_object(Bucket=bucket_name, Key=thumbnail_key)

            print(f"[INFO] Miniatura movida a: {nueva_ruta_thumbnail}")

            # Actualizar la base de datos
            session = get_db_session()
            update_query = text(f"""
                UPDATE {tabla_guardada}
                SET imagen = :imagen
                WHERE fid = :fid
            """)
            imagen_metadata = json.dumps({"thumbnail": nueva_ruta_thumbnail})
            session.execute(update_query, {"imagen": imagen_metadata, "fid": id_tabla})
            session.commit()
            session.close()

            print(f"[INFO] Base de datos actualizada en {tabla_guardada}, ID: {id_tabla}")
        else:
            print(f"[WARNING] Miniatura aún no está disponible. Manteniéndola en pendientes.")
            updated_pending_thumbnails.append(thumbnail)

    # Guardar solo las miniaturas que siguen pendientes
    save_pending_files_to_minio(s3_client, bucket_name, pending_file_key, updated_pending_thumbnails)
    print("[INFO] Procesamiento de miniaturas pendientes finalizado.")

# Procesa el mensaje de Kafka
def process_thumbnail_message(message, **kwargs):
    """Procesa el mensaje del tópico `thumbs`."""
    print(f"[INFO] Mensaje recibido: {message}")

    try:
        raw_message = message.value()
        if not raw_message:
            print("[ERROR] No se encontró contenido en el mensaje.")
            return

        msg = json.loads(raw_message.decode('utf-8'))
        value = msg.get("value")
        if not value:
            print("[ERROR] El mensaje no contiene el campo 'value'.")
            return

        ruta_imagen_original = value.get("RutaImagen")
        id_tabla = value.get("IdDeTabla")
        tabla_guardada = value.get("TablaGuardada")

        if not ruta_imagen_original or not id_tabla or not tabla_guardada:
            print(f"[ERROR] El mensaje está incompleto: {msg}")
            return

        print(f"[INFO] Datos procesados: RutaImagen={ruta_imagen_original}, IdDeTabla={id_tabla}, TablaGuardada={tabla_guardada}")

        # Guardar en pendientes
        connection = BaseHook.get_connection('minio_conn')
        extra = json.loads(connection.extra)
        s3_client = boto3.client(
            's3',
            endpoint_url=extra['endpoint_url'],
            aws_access_key_id=extra['aws_access_key_id'],
            aws_secret_access_key=extra['aws_secret_access_key'],
            config=Config(signature_version='s3v4')
        )
        
        bucket_name = "tmp"
        pending_file_key = "pending_thumbnails.json"
        
        pending_thumbnails = load_pending_files_from_minio(s3_client, bucket_name, pending_file_key)
        pending_thumbnails.append({
            "RutaImagen": ruta_imagen_original,
            "IdDeTabla": id_tabla,
            "TablaGuardada": tabla_guardada
        })

        save_pending_files_to_minio(s3_client, bucket_name, pending_file_key, pending_thumbnails)
        print("[INFO] Miniatura agregada a la lista de pendientes.")

        # Esperar 10 segundos antes de continuar para evitar sobrecarga
        time.sleep(10)

    except Exception as e:
        print(f"[ERROR] Error no manejado: {e}")
        raise e


# Configuración del DAG
default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'process_thumbnail_and_update_db',
    default_args=default_args,
    description='Procesa miniaturas y actualiza la base de datos con reintentos',
    schedule_interval='*/3 * * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=3  
)

check_pending_task = PythonOperator(
    task_id='check_pending_thumbnails',
    python_callable=process_pending_thumbnails,
    dag=dag,
)

consume_thumbs_topic = ConsumeFromTopicOperator(
    kafka_config_id="kafka_connection",
    task_id="consume_thumbs_topic",
    topics=["thumbs"],
    apply_function=process_thumbnail_message,
    apply_function_kwargs={},
    commit_cadence="end_of_operator",
    dag=dag,
)

check_pending_task >> consume_thumbs_topic
