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
        # Extraer el mensaje
        raw_message = message.value()
        print(f"[DEBUG] Raw message: {raw_message}")
        if not raw_message:
            print("[ERROR] No se encontró contenido en el mensaje.")
            return

        # Decodificar el JSON del mensaje
        try:
            value = json.loads(raw_message.decode('utf-8'))
            print(f"[INFO] Mensaje decodificado como JSON: {value}")
        except json.JSONDecodeError as e:
            print(f"[ERROR] Error al decodificar el JSON: {e}")
            return

        # Extraer valores
        ruta_imagen_original = value.get("RutaImagen")
        id_tabla = value.get("IdDeTabla")
        tabla_guardada = value.get("TablaGuardada")

        if not ruta_imagen_original or not id_tabla or not tabla_guardada:
            print(f"[ERROR] El mensaje está incompleto: {value}")
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
        thumbnail_key = f"thumbs/{nombre_archivo}"
        nueva_ruta_thumbnail = f"{carpeta_original}/{nombre_archivo}"

        print(f"[DEBUG] Thumbnail key: {thumbnail_key}")
        print(f"[DEBUG] Nueva ruta de thumbnail: {nueva_ruta_thumbnail}")

        # Verificar existencia del archivo en MinIO antes de moverlo
        print("[INFO] Verificando existencia del archivo en MinIO.")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=thumbnail_key)
        if 'Contents' not in response:
            print(f"[ERROR] El archivo '{thumbnail_key}' no existe en el bucket '{bucket_name}'.")
            return

        # Mover la miniatura en MinIO
        print(f"[INFO] Archivo encontrado. Procediendo a mover la miniatura.")
        copy_source = {"Bucket": bucket_name, "Key": thumbnail_key}
        s3_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=nueva_ruta_thumbnail)
        s3_client.delete_object(Bucket=bucket_name, Key=thumbnail_key)
        print(f"[INFO] Miniatura movida a: {nueva_ruta_thumbnail}")

                # Determinar la acción según el tipo de evento (tabla)
        session = get_db_session()

        if tabla_guardada == "observacion_aerea.observation_captura_video":
            # Obtener el valor actual de 'video'
            existing_data = session.execute(
                text(f"SELECT video FROM {tabla_guardada} WHERE fid = :fid"), {"fid": id_tabla}
            ).scalar()

            # Convertir a JSON y agregar nuevo thumbnail
            existing_json = json.loads(existing_data) if existing_data else {}
            existing_json["thumbnail"] = nueva_ruta_thumbnail

            # Actualizar en la base de datos
            update_query = text(f"""
                UPDATE {tabla_guardada}
                SET video = :video
                WHERE fid = :fid
            """)
            session.execute(update_query, {"video": json.dumps(existing_json), "fid": id_tabla})

        elif tabla_guardada in [
            "observacion_aerea.observation_captura_imagen_visible",
            "observacion_aerea.observation_captura_imagen_infrarroja",
            "observacion_aerea.observation_captura_imagen_multiespectral"
        ]:
            # Obtener el valor actual de 'imagen'
            existing_data = session.execute(
                text(f"SELECT imagen FROM {tabla_guardada} WHERE fid = :fid"), {"fid": id_tabla}
            ).scalar()

            # Convertir a JSON y agregar nuevo thumbnail
            existing_json = json.loads(existing_data) if existing_data else {}
            existing_json["thumbnail"] = nueva_ruta_thumbnail

            # Actualizar en la base de datos
            update_query = text(f"""
                UPDATE {tabla_guardada}
                SET imagen = :imagen
                WHERE fid = :fid
            """)
            session.execute(update_query, {"imagen": json.dumps(existing_json), "fid": id_tabla})

        elif tabla_guardada in [
            "observacion_aerea.observation_captura_rafaga_visible",
            "observacion_aerea.observation_captura_rafaga_infrarroja",
            "observacion_aerea.observation_captura_rafaga_multiespectral"
        ]:
            # Obtener el valor actual de 'temporal_subsamples'
            existing_data = session.execute(
                text(f"SELECT temporal_subsamples FROM {tabla_guardada} WHERE fid = :fid"), {"fid": id_tabla}
            ).scalar()

            # Convertir a JSON y agregar nuevo thumbnail
            existing_json = json.loads(existing_data) if existing_data else {}
            existing_json["thumbnail"] = nueva_ruta_thumbnail

            # Actualizar en la base de datos
            update_query = text(f"""
                UPDATE {tabla_guardada}
                SET temporal_subsamples = :temporal_subsamples
                WHERE fid = :fid
            """)
            session.execute(update_query, {"temporal_subsamples": json.dumps(existing_json), "fid": id_tabla})

        else:
            print(f"[ERROR] Tipo de evento no reconocido: {tabla_guardada}")
            return

        session.commit()
        session.close()
        print(f"[INFO] Base de datos actualizada en {tabla_guardada}, ID: {id_tabla}")

    except Exception as e:
        print(f"[ERROR] Error al procesar el mensaje: {e}")
        return

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
    schedule_interval='*/5 * * * *',
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
