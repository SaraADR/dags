import json
import os
import boto3
from botocore.client import Config
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import text
from dag_utils import get_db_session, get_minio_client


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
        s3_client = get_minio_client()
        bucket_name = "tmp"

        # Generar la nueva ruta para la miniatura
        # Generar la nueva ruta para la miniatura
        nombre_archivo = os.path.basename(ruta_imagen_original)
        carpeta_original = os.path.dirname(ruta_imagen_original)

        # Cambia la extensión a .jpg
        thumbnail_key = f"thumbs/{os.path.splitext(nombre_archivo)[0]}_thumb.jpg"
        nueva_ruta_thumbnail = f"{carpeta_original}/{os.path.splitext(nombre_archivo)[0]}_thumb.jpg"


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

        # Conectar a la base de datos
        session = get_db_session()

        # Determinar la acción según el tipo de evento (tabla)
        if tabla_guardada == "observacion_aerea.observation_captura_video":
            update_query = text("""
                UPDATE observacion_aerea.observation_captura_video
                SET video = video || :video
                WHERE fid = :fid
            """)
            video_metadata = json.dumps({"thumbnail": nueva_ruta_thumbnail})
            session.execute(update_query, {"video": video_metadata, "fid": id_tabla})

        elif tabla_guardada in [
            "observacion_aerea.observation_captura_imagen_visible",
            "observacion_aerea.observation_captura_imagen_infrarroja",
            "observacion_aerea.observation_captura_imagen_multiespectral"
        ]:
            update_query = text(f"""
                UPDATE {tabla_guardada}
                SET imagen = imagen || :imagen
                WHERE fid = :fid
            """)
            imagen_metadata = json.dumps({"thumbnail": nueva_ruta_thumbnail})
            session.execute(update_query, {"imagen": imagen_metadata, "fid": id_tabla})

        elif tabla_guardada in [
            "observacion_aerea.observation_captura_rafaga_visible",
            "observacion_aerea.observation_captura_rafaga_infrarroja",
            "observacion_aerea.observation_captura_rafaga_multiespectral"
        ]:
            update_query = text(f"""
                UPDATE {tabla_guardada}
                SET temporal_subsamples = temporal_subsamples || :temporal_subsamples
                WHERE fid = :fid
            """)
            temporal_metadata = json.dumps({"thumbnail": nueva_ruta_thumbnail})
            session.execute(update_query, {"temporal_subsamples": temporal_metadata, "fid": id_tabla})

        else:
            print(f"[ERROR] Tipo de evento no reconocido: {tabla_guardada}")
            return

        session.commit()

        # Enviar notificación al sistema de misiones
        notification_message = f"Un nuevo recurso multimedia ha sido agregado a la misión {id_tabla}."
        insert_notification(id_tabla, notification_message)


        session.close()
        print(f"[INFO] Base de datos actualizada en {tabla_guardada}, ID: {id_tabla}")

    except Exception as e:
        print(f"[ERROR] Error no manejado: {e}")
        raise e
    
    
def insert_notification(id_mission, message):
    """Inserta una notificación en la base de datos para actualizar la misión en el front-end."""
    if id_mission is not None:
        try:
            session = get_db_session()           
            engine = session.get_bind()

            data_json = json.dumps({
                "to": "all_users",
                "actions": [
                    {
                        "type": "reloadMissionElements",
                        "data": {
                            "missionId": id_mission,
                            "elements": ["multimedia"]
                        }
                    },
                    {
                        "type": "notify",
                        "data": {
                            "message": message
                        }
                    }
                ]
            }, ensure_ascii=False)

            time = datetime.now().replace(tzinfo=timezone.utc)

            query = text("""
                INSERT INTO public.notifications
                (destination, "data", "date", status)
                VALUES (:destination, :data, :date, NULL);
            """)
            session.execute(query, {
                'destination': 'ignis',
                'data': data_json,
                'date': time
            })
            session.commit()

            print(f"[INFO] Notificación enviada para la misión {id_mission}")

        except Exception as e:
            session.rollback()
            print(f"[ERROR] Error al insertar notificación: {str(e)}")
        finally:
            session.close()


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
