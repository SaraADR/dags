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

    conf = kwargs.get('dag_run').conf
    print(f"[DEBUG] Conf recibida: {conf}")
    
    if not conf or 'output_json' not in conf:
        print("[ERROR] No se recibió 'output_json', abortando ejecución.")
        return

    output_json = conf['output_json']
    if isinstance(output_json, str):
        output_json = json.loads(output_json)
    print(f"[DEBUG] output_json recibido:\n{json.dumps(output_json, indent=2)}")

    minio_img_url = conf.get("RutaImagen", None)
    print(f"[INFO] URL de imagen (MinIO): {minio_img_url}")

    session = get_db_session()
    print("[INFO] Sesión de base de datos abierta correctamente.")

    minio_base_url = minio_api()
    print(f"[INFO] Conexión a MinIO: {minio_base_url}")

    try:
        model = output_json.get("FileName", "").lower()
        print(f"[DEBUG] Modelo detectado: {model}")

        if model.endswith("-ter.tiff"):
            tipo = "infrarroja"
        elif "-mul-" in model or "-band_" in model:
            tipo = "multiespectral"
        elif "-harrier.tiff" in model or "-basler.tiff" in model:
            tipo = "visible"
        else:
            tipo = "visible"
        print(f"[INFO] Tipo de ráfaga detectado: {tipo}")

        tabla_captura = f"observacion_aerea.captura_rafaga_{tipo}"
        tabla_observacion = f"observacion_aerea.observation_captura_rafaga_{tipo}"
        tabla_imagen = f"observacion_aerea.observation_captura_imagen_{tipo}"

        rafaga_id = output_json.get("IdentificadorRafaga")
        mission_id = output_json.get("MissionID")

        dt_actual = parse_date(output_json.get("DateTimeOriginal")).replace(tzinfo=None)
        valid_time_start = dt_actual
        valid_time_end = dt_actual + timedelta(minutes=1)

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

        print("[INFO] Buscando si ya existe ráfaga reciente con la misma matrícula...")
        matricula = output_json.get("AircraftNumberPlate")
        check_sql = text(f"""
            SELECT fid
            FROM {tabla_captura}
            WHERE platform = :matricula
            AND upper(valid_time) > now() - interval '5 seconds'
            ORDER BY fid DESC
            LIMIT 1
        """)
        existente = session.execute(check_sql, {"matricula": matricula}).fetchone()

        if existente:
            captura_fid = existente.fid
            print(f"[INFO] Ráfaga existente con fid: {captura_fid}, actualizando tiempo.")
            update_sql = f"""
                UPDATE {tabla_captura}
                SET valid_time = tsrange(lower(valid_time), :end_time)
                WHERE fid = :fid
            """
            session.execute(text(update_sql), {"fid": captura_fid, "end_time": valid_time_end})
        else:
            print("[INFO] Insertando nueva ráfaga...")
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
            print(f"[OK] Ráfaga insertada con fid: {captura_fid}")

        try:
            lat = float(output_json.get("GPSLatitude", "0").split()[0])
            lon = float(output_json.get("GPSLongitude", "0").split()[0])
            offset = 0.0001
            shape_wkt = (
                f"POLYGON(({lon - offset} {lat - offset}, "
                f"{lon - offset} {lat + offset}, "
                f"{lon + offset} {lat + offset}, "
                f"{lon + offset} {lat - offset}, "
                f"{lon - offset} {lat - offset}))"
            )
        except:
            shape_wkt = "POLYGON((0 0,0 0,0 0,0 0,0 0))"

        file_name = output_json.get("FileName", "")
        base_name = os.path.splitext(os.path.basename(file_name))[0]
        thumbnail_key = f"thumbs/{base_name}_thumb.jpg"
        image_url = f"{minio_base_url}/tmp/{thumbnail_key}"
        output_json["image_url"] = image_url

        temporal_subsample_data = dict(output_json)

        insert_obs_sql = f"""
            INSERT INTO {tabla_observacion} (
                procedure, sampled_feature, shape, result_time, phenomenon_time,
                identificador_rafaga, temporal_subsamples
            ) VALUES (
                :procedure, :sampled_feature, ST_GeomFromText(:shape, 4326),
                :result_time, tsrange(:start, :end),
                :identificador_rafaga, :temporal_subsamples
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
            "temporal_subsamples": json.dumps(temporal_subsample_data, ensure_ascii=False)
        })

        output_json["ReadedFromVersion"] = conf.get("version", "desconocida")
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
            "imagen": json.dumps(output_json, ensure_ascii=False)
        })

        session.commit()
        print("[SUCCESS] Todos los datos guardados correctamente.")

    except Exception as e:
        session.rollback()
        print(f"[ERROR] Excepción durante el procesamiento: {e}")
    finally:
        session.close()
        print("[INFO] Sesión de base de datos cerrada.")


# DAG   
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
