import json
import os
import boto3
from botocore.client import Config
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import text
from dag_utils import get_db_session


def process_thumbnail_message(message, **kwargs):
    """Procesa el mensaje del tópico `thumbs`."""
    print(f"[INFO] Mensaje recibido: {message}")

    try:
        # Extraer el mensaje del contexto
        raw_message = message.value()
        print(f"[DEBUG] Raw message: {raw_message}")
        if not raw_message:
            print("[ERROR] No se encontró contenido en el mensaje.")
            return

        # Decodificar el mensaje como JSON
        try:
            msg = json.loads(raw_message.decode('utf-8'))
            print(f"[INFO] Mensaje decodificado como JSON: {msg}")
        except json.JSONDecodeError as e:
            print(f"[ERROR] Error al decodificar el JSON: {e}")
            return

        # Validar que los campos necesarios estén presentes
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

        # Configuración de MinIO
        print("[INFO] Configurando conexión con MinIO.")
        connection = BaseHook.get_connection('minio_conn')
        extra = json.loads(connection.extra)
        print(f"[DEBUG] MinIO extra config: {extra}")
        s3_client = boto3.client(
            's3',
            endpoint_url=extra['endpoint_url'],
            aws_access_key_id=extra['aws_access_key_id'],
            aws_secret_access_key=extra['aws_secret_access_key'],
            config=Config(signature_version='s3v4')
        )
        bucket_name = "tmp"

        # Generar la nueva ruta para la miniatura
        nombre_archivo = os.path.basename(ruta_imagen_original)
        carpeta_original = os.path.dirname(ruta_imagen_original)
        thumbnail_key = f"thumbs/{nombre_archivo.replace('.mp4', 'thumb.jpg')}"
        nueva_ruta_thumbnail = f"{carpeta_original}/{nombre_archivo.replace('.mp4', 'thumb.jpg')}"
        print(f"[DEBUG] Thumbnail key: {thumbnail_key}")
        print(f"[DEBUG] Nueva ruta de thumbnail: {nueva_ruta_thumbnail}")

        # Mover la miniatura en MinIO
        print("[INFO] Verificando existencia del archivo en MinIO.")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=thumbnail_key)
        if 'Contents' not in response:
            print(f"[ERROR] El archivo '{thumbnail_key}' no existe en el bucket '{bucket_name}'.")
            return

        print(f"[INFO] Archivo encontrado. Procediendo a mover la miniatura.")
        copy_source = {"Bucket": bucket_name, "Key": thumbnail_key}
        s3_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=nueva_ruta_thumbnail)
        s3_client.delete_object(Bucket=bucket_name, Key=thumbnail_key)
        print(f"[INFO] Miniatura movida a: {nueva_ruta_thumbnail}")

        # Determinar la tabla correcta
        print("[INFO] Determinando la tabla correcta para actualizar.")
        if "visible" in tabla_guardada:
            tabla_actualizar = "observation_captura_imagen_visible"
        elif "infrarroja" in tabla_guardada:
            tabla_actualizar = "observation_captura_imagen_infrarroja"
        elif "multiespectral" in tabla_guardada:
            tabla_actualizar = "observation_captura_imagen_multiespectral"
        elif "rafaga" in tabla_guardada and "visible" in tabla_guardada:
            tabla_actualizar = "observation_captura_rafaga_visible"
        elif "rafaga" in tabla_guardada and "infrarroja" in tabla_guardada:
            tabla_actualizar = "observation_captura_rafaga_infrarroja"
        elif "rafaga" in tabla_guardada and "multiespectral" in tabla_guardada:
            tabla_actualizar = "observation_captura_rafaga_multiespectral"
        elif "video" in tabla_guardada:
            tabla_actualizar = "observation_captura_video"
        else:
            print(f"[ERROR] Tabla no reconocida en el mensaje: {tabla_guardada}")
            return

        print(f"[INFO] Tabla seleccionada: {tabla_actualizar}")

        # Actualizar la base de datos
        print("[INFO] Actualizando la base de datos.")
        session = get_db_session()
        update_query = text(f"""
            UPDATE {tabla_actualizar}
            SET imagen = :imagen
            WHERE id = :id
        """)
        print(f"[DEBUG] Ejecutando query: {update_query}")
        session.execute(update_query, {"imagen": nueva_ruta_thumbnail, "id": id_tabla})
        session.commit()
        session.close()
        print(f"[INFO] Base de datos actualizada en {tabla_actualizar}, ID: {id_tabla}")

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
    description='Procesa miniaturas y actualiza la base de datos',
    schedule_interval='*/1 * * * *',
    catchup=False,
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

consume_thumbs_topic
