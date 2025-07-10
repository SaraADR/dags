from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import text
import json
from dateutil.parser import parse as parse_date
from dag_utils import get_db_session, minio_api

def insert_rafaga_and_observation(**kwargs):
    print("\n[INFO] Iniciando procesamiento de ráfaga")
    print("[INFO] Verificando configuración de ejecución...")
    

    conf = kwargs.get('dag_run').conf
    print(f"[INFO] Configuración recibida: {conf}")
    if not conf or 'output_json' not in conf:
        print("[ERROR] No se recibió 'output_json', abortando ejecución.")
        return

    output_json = conf['output_json']
    print(f"[INFO] output_json recibido: {output_json}")
    if isinstance(output_json, str):
        output_json = json.loads(output_json)

    ruta_imagen = conf.get("RutaImagen", "")
    print(f"[INFO] Ruta de imagen: {ruta_imagen}")
    session = get_db_session()
    minio_base_url = minio_api()
    print(f"[INFO] URL base de MinIO: {minio_base_url}")

    try:
        model = output_json.get("FileName", "").lower()
        print(f"[INFO] Modelo de archivo: {model}")
        if model.endswith("-ter.tiff"):
            tipo = "infrarroja"
        elif "-mul-" in model or "-band_" in model:
            tipo = "multiespectral"
        elif "-harrier.tiff" in model or "-basler.tiff" in model:
            tipo = "visible"
        else:
            tipo = "visible"
        print(f"[INFO] Tipo de ráfaga determinado: {tipo}")

        tabla_captura = f"observacion_aerea.captura_rafaga_{tipo}"
        tabla_observacion = f"observacion_aerea.observation_captura_rafaga_{tipo}"
        tabla_imagen = f"observacion_aerea.observation_captura_imagen_{tipo}"

        rafaga_id = output_json.get("IdentificadorRafaga")
        mission_id = output_json.get("MissionID")

        # # Verificación para evitar duplicados de ráfaga ya procesada
        # if not rafaga_id:
        #     print("[ERROR] No se encontró 'IdentificadorRafaga'. Abortando.")
        #     return

        # check_existing_rafaga = text(f"""
        #     SELECT 1 FROM {tabla_observacion}
        #     WHERE identificador_rafaga = :rafaga_id
        #     LIMIT 1
        # """)
        # ya_insertada = session.execute(check_existing_rafaga, {"rafaga_id": rafaga_id}).fetchone()
        # if ya_insertada:
        #     print(f"[INFO] La ráfaga con ID '{rafaga_id}' ya ha sido procesada anteriormente. Abortando inserción.")
        #     return


        date_fields = ["DateTimeOriginal", "FileModifyDate", "FileAccessDate", "FileInodeChangeDate"]
        date_str = next((output_json.get(field) for field in date_fields if output_json.get(field)), None)
        if not date_str:
            raise ValueError("No se encontró ninguna fecha válida.")
        dt_actual = parse_date(date_str).replace(tzinfo=None)

        valid_time_start = dt_actual
        valid_time_end = dt_actual + timedelta(minutes=1)

        print(f"[INFO] Tiempo de validez: {valid_time_start} a {valid_time_end}")

        base_params = {
            'payload_id': output_json.get('PayloadSN'),
            'multisim_id': output_json.get('MultisimSN'),
            'ground_control_station_id': output_json.get('GroundControlStationSN'),
            'pc_embarcado_id': output_json.get('PCEmbarcadoSN'),
            'operator_name': output_json.get('OperatorName'),
            'pilot_name': output_json.get('PilotName'),
            'sensor': output_json.get('Model'),
            'platform': output_json.get('AircraftNumberPlate'),
            'start_time': valid_time_start,
            'end_time': valid_time_end
        }

        print(f"[INFO] Parámetros base para inserción: {base_params}")

        matricula = output_json.get("AircraftNumberPlate")
        check_sql = text(f"""
            SELECT fid FROM {tabla_captura}
            WHERE platform = :matricula
            AND upper(valid_time) > now() - interval '3 minutes'
            ORDER BY fid DESC LIMIT 1
        """)
        existente = session.execute(check_sql, {"matricula": matricula}).fetchone()

        print(f"[INFO] Captura existente: {existente}")

        if existente:
            captura_fid = existente.fid
            session.execute(text(f"""
                UPDATE {tabla_captura}
                SET valid_time = tsrange(lower(valid_time), :end_time)
                WHERE fid = :fid
            """), {"fid": captura_fid, "end_time": valid_time_end})
        else:
            insert_sql = f"""
                INSERT INTO {tabla_captura} (
                    valid_time, payload_id, multisim_id, ground_control_station_id,
                    pc_embarcado_id, operator_name, pilot_name, sensor, platform
                ) VALUES (
                    tsrange(:start_time, :end_time),
                    :payload_id, :multisim_id, :ground_control_station_id,
                    :pc_embarcado_id, :operator_name, :pilot_name, :sensor, :platform
                ) RETURNING fid
            """
            result = session.execute(text(insert_sql), base_params)
            captura_fid = result.fetchone()[0]
            print(f"[INFO] Nueva captura insertada con fid: {captura_fid}")

        try:
            lat = float(output_json.get("GPSLatitude", "0").split()[0])
            lon = float(output_json.get("GPSLongitude", "0").split()[0])
            offset = 0.0001
            shape_wkt = (
                f"POLYGON(({lon - offset} {lat - offset}, {lon - offset} {lat + offset}, "
                f"{lon + offset} {lat + offset}, {lon + offset} {lat - offset}, "
                f"{lon - offset} {lat - offset}))"
            )
        except:
            shape_wkt = "POLYGON((0 0,0 0,0 0,0 0,0 0))"
            
            print("[WARNING] No se pudo determinar la ubicación GPS, usando POLYGON(0 0,0 0,0 0,0 0,0 0)")

        file_name = output_json.get("FileName", "")
        base_name = os.path.splitext(os.path.basename(file_name))[0]
        thumbnail_key = f"thumbs/{base_name}_thumb.jpg"
        image_url = f"{minio_base_url}/tmp/{thumbnail_key}"
        original_url = f"{minio_base_url}/tmp/{ruta_imagen}"

        print(f"[INFO] URL de imagen original: {ruta_imagen}")
        print(f"[INFO] URL de imagen original: {original_url}")
        print(f"[INFO] URL de imagen con miniatura: {image_url}")
      

        key_translation = {
            "PN": "PilotName", "NT": "OperatorName", "PSN": "PayloadSN", "MSN": "MultisimSN",
            "GCS": "GroundControlStationSN", "PCE": "PCEmbarcadoSN", "ANP": "AircraftNumberPlate",
            "MD": "Model", "SID": "SensorID", "MID": "MissionID", "BNP": "BateaNumberPlate",
            "VNP": "VesselNumberPlate", "LAT": "GPSLatitude", "LON": "GPSLongitude",
            "ALT": "GPSAltitude", "GPSLAT": "GPSLatitude", "GPSLON": "GPSLongitude",
            "GALT": "GPSAltitude", "POS": "GPSPosition", "DTL": "DistanceDetectLaser",
            "LDL": "LatitudeDetectLaser", "LODL": "LongitudeDetectLaser", "ADL": "AltitudeDetectLaser",
            "IAM": "InfraredAmbientTemp", "IME": "InfraredMinTemperature",
            "IMA": "InfraredMaxTemperature", "IAV": "InfraredAverageTemperature",
            "IDE": "InfraredDeviationTemperature", "IEC": "InfraredEmissivity",
            "ICP": "InfraredCalibrationId", "ICD": "InfraredCalibrationDescription",
            "ICRMIN": "InfraredCalibrationRangeMin", "ICRMAX": "InfraredCalibrationRangeMax",
            "AP": "AircraftPitch", "AR": "AircraftRoll", "AY": "AircraftYaw",
            "IP": "ImagePitch", "IR": "ImageRoll", "IY": "ImageYaw",
            "IQ0": "ImageQuaternion0", "IQ1": "ImageQuaternion1",
            "IQ2": "ImageQuaternion2", "IQ3": "ImageQuaternion3",
            "VTX": "ImageVectorToX", "VTY": "ImageVectorToY", "VTZ": "ImageVectorToZ",
            "VUX": "ImageVectorUpX", "VUY": "ImageVectorUpY", "VUZ": "ImageVectorUpZ",
            "GP": "GimbalPan", "GT": "GimbalTilt", "TAGR": "TagR", "TAGB": "TagB",
            "TAGF": "TagF", "TAGO": "TagO", "J1": "J1", "J0": "J0", "OINT": "OInt",
            "GS": "GroundSamplingDistance", "KNC": "KnotsCount", "TMPRES": "TemperatureResolution",
            "EXTTEMP": "ExtOpticsTemp", "EXTTR": "TransmissionExtOptics",
            "REFTEMP": "ReflectedTemperature", "WINTEMP": "WindowTemperature"
        }

        print("[INFO] Procesando metadatos de ráfaga e imagen...")
        print(f"[INFO] Parámetros de imagen: {output_json.get('ImageParameters', {})}")

        imagen_metadatos = {
            "type": tipo,
            "original": original_url,
            "url": image_url,
            "version": conf.get("version", "desconocida")
        }

        for k, v in output_json.items():
            translated_key = key_translation.get(k, k)
            imagen_metadatos[translated_key] = v

        # output_json["imagen"] = imagen_metadatos

        insert_obs_sql = f"""
            INSERT INTO {tabla_observacion} (
                procedure, sampled_feature, shape, result_time, phenomenon_time,
                identificador_rafaga, imagen
            ) VALUES (
                :procedure, :sampled_feature, ST_GeomFromText(:shape, 4326),
                :result_time, tsrange(:start, :end),
                :identificador_rafaga, :imagen
            )
        """
        session.execute(text(insert_obs_sql), {
            "procedure": int(output_json.get("SensorID", 0)),
            "sampled_feature": mission_id,
            "shape": shape_wkt,
            "result_time": valid_time_start,
            "start": valid_time_start,
            "end": valid_time_end,
            "identificador_rafaga": rafaga_id,
            "imagen": json.dumps(imagen_metadatos, ensure_ascii=False)
        })

        insert_img_sql = f"""
            INSERT INTO {tabla_imagen} (
                shape, sampled_feature, procedure, result_time, phenomenon_time, imagen
            ) VALUES (
                ST_GeomFromText(:shape, 4326), :sampled_feature, :procedure,
                :result_time, :phenomenon_time, :imagen
            )
        """
        session.execute(text(insert_img_sql), {
            "shape": shape_wkt,
            "sampled_feature": mission_id,
            "procedure": int(output_json.get("SensorID", 0)),
            "result_time": valid_time_start,
            "phenomenon_time": valid_time_start,
            "imagen": json.dumps(imagen_metadatos, ensure_ascii=False)
        })

        session.commit()
        print("[SUCCESS] Todos los datos guardados correctamente.")

    except Exception as e:
        session.rollback()
        print(f"[ERROR] Excepción durante el procesamiento: {e}")
    finally:
        session.close()

default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'process_rafagas_and_metadatos',
    default_args=default_args,
    description='Guarda metadatos generales de ráfaga + imagen individual',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

process_task = PythonOperator(
    task_id='process_rafagas',
    python_callable=insert_rafaga_and_observation,
    provide_context=True,
    dag=dag,
)
