import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
import json
from sqlalchemy import text
from dag_utils import get_db_session, get_minio_client, minio_api, upload_to_minio_path
import uuid

# Traducción de claves
key_translation = {
    "PN": "PilotName", "NT": "OperatorName", "PSN": "PayloadSN", "MSN": "MultisimSN",
    "GCS": "GroundControlStationSN", "PCE": "PCEmbarcadoSN", "ANP": "AircraftNumberPlate",
    "MD": "Model", "SID": "SensorID", "MID": "MissionID", "BNP": "BateaNumberPlate",
    "VNP": "VesselNumberPlate", "LAT": "GPSLatitude", "LON": "GPSLongitude", "ALT": "GPSAltitude",
    "GPSLAT": "GPSLatitude", "GPSLON": "GPSLongitude", "GALT": "GPSAltitude", "POS": "GPSPosition",
    "DTL": "DistanceDetectLaser", "LDL": "LatitudeDetectLaser", "LODL": "LongitudeDetectLaser",
    "ADL": "AltitudeDetectLaser", "IAM": "InfraredAmbientTemp", "IME": "InfraredMinTemperature",
    "IMA": "InfraredMaxTemperature", "IAV": "InfraredAverageTemperature", "IDE": "InfraredDeviationTemperature",
    "IEC": "InfraredEmissivity", "ICP": "InfraredCalibrationId", "ICD": "InfraredCalibrationDescription",
    "ICRMIN": "InfraredCalibrationRangeMin", "ICRMAX": "InfraredCalibrationRangeMax", "AP": "Platform",
    "AR": "AircraftRoll", "AY": "AircraftYaw", "IP": "ImagePitch", "IR": "ImageRoll",
    "IY": "ImageYaw", "IQ0": "ImageQuaternion0", "IQ1": "ImageQuaternion1", "IQ2": "ImageQuaternion2",
    "IQ3": "ImageQuaternion3", "VTX": "ImageVectorToX", "VTY": "ImageVectorToY", "VTZ": "ImageVectorToZ",
    "VUX": "ImageVectorUpX", "VUY": "ImageVectorUpY", "VUZ": "ImageVectorUpZ", "GP": "GimbalPan",
    "GT": "GimbalTilt", "TAGR": "TagR", "TAGB": "TagB", "TAGF": "TagF", "TAGO": "TagO",
    "J1": "J1", "J0": "J0", "OINT": "OInt", "GS": "GroundSamplingDistance", "KNC": "KnotsCount",
    "TMPRES": "TemperatureResolution", "EXTTEMP": "ExtOpticsTemp", "EXTTR": "TransmissionExtOptics",
    "REFTEMP": "ReflectedTemperature", "WINTEMP": "WindowTemperature",
    "DV": "DurationVideo", "MN": "MissionID", "MT": "MissionType",
    "OD": "ObservationDetail", "SM": "SensorModel", "IEB": "InfraredEmitterBias", "OP": "OperatorName",
}

def translate_keys(data):
    if isinstance(data, dict):
        return {key_translation.get(k, k): translate_keys(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [translate_keys(item) for item in data]
    else:
        return data
    
def extract_and_translate_all_keys(d, prefix=""):
    flat_result = {}
    if isinstance(d, dict):
        for k, v in d.items():
            translated_key = key_translation.get(k, k)
            if isinstance(v, (dict, list)):
                nested = extract_and_translate_all_keys(v, prefix=f"{prefix}{translated_key}.")
                flat_result.update(nested)
            else:
                flat_result[f"{prefix}{translated_key}"] = v
    elif isinstance(d, list):
        for i, item in enumerate(d):
            nested = extract_and_translate_all_keys(item, prefix=f"{prefix}{i}.")
            flat_result.update(nested)
    return flat_result

def process_video_and_metadatos(**context):
    conf = context['dag_run'].conf
    output_json = conf.get("output_json", {})
    ruta_video = conf.get("RutaVideo", "video_tipo2.mp4")
    message = conf.get("message")
    version = conf.get("version", "1.0.5")
    tipo_video = conf.get("type", "video_tipo2")

    comment_data = output_json.get("Comment", {})
    if isinstance(comment_data, str):
        try:
            comment_data = json.loads(comment_data)
        except:
            comment_data = {}

    general_raw = comment_data.get("general", {})
    frame_raw = comment_data.get("data", [])

    def looks_translated(d):
        return any(k in d for k in ["Platform", "MissionID", "PilotName", "SensorModel"])

    # Detecta si al menos una clave abreviada está presente
    abreviadas_presentes = any(k in general_raw for k in key_translation.keys())

    if abreviadas_presentes:
        print("Se detectaron claves abreviadas. Se aplica translate_keys.")
        general_data = translate_keys(general_raw)
        frame_data = translate_keys(frame_raw)
    else:
        print("No se detectaron claves abreviadas. No se aplica translate_keys.")
        general_data = general_raw
        frame_data = frame_raw


    # Prints de control
    print("general_data resultante:")
    print(json.dumps(general_data, indent=2, ensure_ascii=False))

    print("frame_data[0] (si existe):")
    if isinstance(frame_data, list) and frame_data:
        print(json.dumps(frame_data[0], indent=2, ensure_ascii=False))
    else:
        print("frame_data vacío o malformado.")

    date_time_str = output_json.get("xmp:dateTimeOriginal") or output_json.get("CreateDate")
    try:
        timestamp_naive = datetime.strptime(date_time_str, "%Y-%m-%dT%H:%M")
    except:
        timestamp_naive = datetime.now(timezone.utc).replace(tzinfo=None)

    duration_in_seconds = 30
    if general_data.get("DurationVideo"):
        try:
            duration_in_seconds = int(general_data["DurationVideo"])
        except:
            pass

    valid_time_end = timestamp_naive + timedelta(seconds=duration_in_seconds)
    minio_url = f"{minio_api()}/{ruta_video}"
    video_filename = os.path.basename(ruta_video)
    video_name, _ = os.path.splitext(video_filename)
    thumb_filename = f"{video_name}_thumb.jpg"
    image_url = f"{minio_api()}/tmp/thumbs/{thumb_filename}"

    print(f"URL de la miniatura: {image_url}")

    # Aplanar y traducir todos los datos
    flat_data = extract_and_translate_all_keys(conf)
    print("Claves traducidas y aplanadas:")
    for k, v in flat_data.items():
        print(f" - {k}: {v}")


   
    # Extraer solo las claves de output_json.general.*
    general_data_from_flat = {
        k.replace("output_json.general.", ""): v
        for k, v in flat_data.items()
        if k.startswith("output_json.general.")
    }

    # Agregar otros campos útiles no incluidos directamente antes
    additional_flat_fields = {
        k.replace("output_json.", ""): v
        for k, v in flat_data.items()
        if k.startswith("output_json.") and not k.startswith("output_json.data.") and not k.startswith("output_json.general.")
    }

    # Determinar el tipo de video en función del nombre
    video_kind = "ClipCorto"
    video_segment = None

    if "_" in video_name:
        possible_suffix = video_name.split("_")[-1]
        if possible_suffix.isdigit():
            video_kind = "OperacionCompleta"
            video_segment = int(possible_suffix)

    # Construir el JSON final con solo los datos relevantes
    new_json = {
        "FileName": video_filename,
        "FileNameThumb": thumb_filename,
        "type": tipo_video,
        "url": image_url,
        "original": minio_url,
        "version": version,
        "DurationVideo": duration_in_seconds,
        "xmp:dateTimeOriginal": timestamp_naive.isoformat(),
        "Frames": frame_data,
        "VideoType": video_kind,
        "VideoDuration": video_segment,
        **additional_flat_fields,
        **general_data_from_flat,
    }



    # Añadir campos normalizados desde general_data

    print("Nuevo JSON generado y listo para insertar:")
    print(json.dumps(new_json, indent=2, ensure_ascii=False))

    # INSERCIONES ORIGINALES
    session = get_db_session()
    try:
        insert_captura_query = text("""
            INSERT INTO observacion_aerea.captura_video
            (valid_time, payload_id, multisim_id, 
            ground_control_station_id, pc_embarcado_id, operator_name, pilot_name, 
            sensor, platform)
            VALUES (tsrange(:valid_time_start, :valid_time_end, '[)'), :payload_id, :multisim_id, 
                    :ground_control_station_id, :pc_embarcado_id, :operator_name, :pilot_name, 
                    :sensor, :platform)
            ON CONFLICT DO NOTHING
        """)

        print("Valores para insertar en captura_video:")
        for field in ['PSN', 'MSN', 'GCS', 'PCE', 'ON', 'PilotName', 'SensorID', 'Platform']:
            print(f" - {field}: {new_json.get(field)}")


        session.execute(insert_captura_query, {
            'valid_time_start': timestamp_naive,
            'valid_time_end': valid_time_end,
            'payload_id': new_json.get("PSN"),
            'multisim_id': new_json.get("MSN"),
            'ground_control_station_id': new_json.get("GCS"),
            'pc_embarcado_id': new_json.get("PCE"),
            'operator_name': new_json.get("ON"),
            'pilot_name': new_json.get("PilotName") or "PilotoDesconocido",
            'sensor': new_json.get("SensorModel") or "SensorDesconocido",
            'platform': new_json.get("Platform")
        })

        shape = None
        if frame_data and isinstance(frame_data, list):
            first = frame_data[0]
            lat = first.get("LAT")
            lon = first.get("LON")
            if lat and lon:
                offset = 0.0001
                coords = [
                    (lon - offset, lat - offset),
                    (lon - offset, lat + offset),
                    (lon + offset, lat + offset),
                    (lon + offset, lat - offset),
                    (lon - offset, lat - offset)
                ]
                coords_str = ", ".join(f"{x} {y}" for x, y in coords)
                shape = f"SRID=4326;POLYGON (({coords_str}))"

        insert_observacion_query = text("""
            INSERT INTO observacion_aerea.observation_captura_video
            (procedure, sampled_feature, shape, result_time, phenomenon_time, video)
            VALUES (:procedure, :sampled_feature, :shape, :result_time, 
                    tsrange(:valid_time_start, :valid_time_end, '[)'), :video)
            RETURNING fid;
        """)

        result = session.execute(insert_observacion_query, {
            "procedure": new_json.get("SensorModel") or "0",
            "sampled_feature": new_json.get("MissionID"),
            "shape": shape,
            "result_time": timestamp_naive,
            "valid_time_start": timestamp_naive,
            "valid_time_end": valid_time_end,
            "video": json.dumps(new_json, ensure_ascii=False, indent=4)
        })

        new_id = result.scalar()
        session.commit()

        if message:
            file_name = os.path.basename(message)
            key = str(uuid.uuid4())
            minio_client = get_minio_client()
            upload_to_minio_path(minio_client, 'missions', f"{new_json.get('MissionID')}/{key}/{file_name}", message)
            print(f"Video subido a MinIO: missions/{new_json.get('MissionID')}/{key}/{file_name}")

        print(f"Metadatos insertados con ID: {new_id}")
    except Exception as e:
        session.rollback()
        print(f"Error durante el procesamiento: {str(e)}")
        raise
    finally:
        session.close()

default_args = {
    'owner': 'Oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'process_video_and_metadatos',
    default_args=default_args,
    description='DAG para procesar videos tipo 2 con normalización de metadatos',
    schedule_interval=None,
    catchup=False,
)

process_task = PythonOperator(
    task_id='process_video_and_metadatos',
    python_callable=process_video_and_metadatos,
    provide_context=True,
    dag=dag,
)
