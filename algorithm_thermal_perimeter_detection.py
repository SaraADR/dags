import json
import os
import time
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
from dag_utils import get_minio_client, throw_job_error, update_job_status
from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook
import shutil
from dag_utils import execute_query
import pytz

# Función que ejecuta el algoritmo de generación de perímetros térmicos utilizando los datos recibidos desde la interfaz y ejecuta el Docker.
def execute_thermal_perimeter_process(**context):

    conf = context.get("dag_run").conf
    if not conf:
        print("[ERROR] No se recibió configuración desde el DAG.")
        return
    
    print("[INFO] Datos recibidos del DAG:")
    print("[INFO]", json.dumps(conf, indent=4))

    message = conf.get("message", {})
    
    job_id = message.get('id')
    input_data_str = message.get('input_data', '{}')
    
    try:
        input_data = json.loads(input_data_str)
    except json.JSONDecodeError:
        print("[ERROR] Error al decodificar 'input_data'")
        throw_job_error(job_id, "Invalid JSON in input_data")
        return

    # Extraer datos básicos de la interfaz
    mission_id = input_data.get('mission_id')
    selected_bursts = input_data.get('selected_bursts', [])
    selected_images = input_data.get('selected_images', [])
    advanced_params = input_data.get('advanced_params', {})
    
    if not mission_id:
        print("[ERROR] No se especificó ID de misión")
        throw_job_error(job_id, "No se especificó ID de misión")
        return
    
    if not selected_bursts:
        print("[ERROR] No se seleccionaron ráfagas")
        throw_job_error(job_id, "No se seleccionaron ráfagas")
        return

    print("[INFO] Datos extraídos correctamente:")
    print(f"[INFO] Mission ID: {mission_id}")
    print(f"[INFO] Selected bursts: {selected_bursts}")
    print(f"[INFO] Selected images: {len(selected_images) if selected_images else 'todas'}")
    print(f"[INFO] Advanced params: {advanced_params}")
    
    # Crear configuración básica para el algoritmo
    algorithm_config = create_basic_config(mission_id, selected_bursts, advanced_params, job_id)
    
    # Ejecutar Docker (pasar también los datos originales para acceso a URLs)
    try:
        execute_docker_algorithm(algorithm_config, job_id, input_data)
        print("[INFO] Algoritmo Docker ejecutado correctamente")
    except Exception as e:
        print(f"[ERROR] Error ejecutando algoritmo: {str(e)}")
        throw_job_error(job_id, str(e))
        raise

# Crea la configuración básica para el algoritmo R según los parámetros recibidos.
def create_basic_config(mission_id, selected_bursts, advanced_params, job_id):
    
    # Parámetros avanzados sin valores por defecto
    n_clusters = advanced_params.get('numberOfClusters')
    threshold_temp = advanced_params.get('tresholdTemp')
    
    if n_clusters is None or threshold_temp is None:
        raise Exception("Faltan parámetros avanzados numberOfClusters o tresholdTemp")
    
    infrared_bursts = []
    for burst_data in selected_bursts:
        if isinstance(burst_data, dict):
            # Estructura con imágenes por ráfaga
            burst_id = burst_data.get('burst_id')
            timestamp = burst_data.get('timestamp', "2024-07-16T12:00:00Z")
            selected_images = burst_data.get('selected_images', [])
        else:
            # Estructura simple
            burst_id = burst_data
            timestamp = "2024-07-16T12:00:00Z"
            selected_images = []
        
        burst_config = {
            "path": f"/share_data/input/incendio{mission_id}/{burst_id}",
            "id": f"PO{burst_id}",
            "timestamp": timestamp,
            "sensorId": "ThermalSensor",
            "imageCount": 5,  # Se actualiza después
            "selected_images": selected_images  # ← Imágenes específicas de esta ráfaga
        }
        infrared_bursts.append(burst_config)
    
    config = {
        "fireId": mission_id,
        "criteria": {
            "numberOfClusters": n_clusters,
            "tresholdTemp": threshold_temp
        },
        "infraredBursts": infrared_bursts
    }
    
    return config

# Ejecuta el algoritmo de perímetros térmicos usando Docker en el servidor remoto.
def execute_docker_algorithm(config, job_id, input_data):
    
    print("[INFO] Iniciando ejecución Docker...")
    
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')
    
    with ssh_hook.get_conn() as ssh_client:
        sftp = ssh_client.open_sftp()
        
        # Rutas en el servidor remoto
        remote_base_dir = '/home/admin3/Algoritmo_deteccion_perimetro'
        config_file = f'{remote_base_dir}/share_data/input/config_{job_id}.json'
        launch_dir = f'{remote_base_dir}/launch'
        
        # Extraer mission_id del config para el directorio de salida
        mission_id = config.get('fireId', 'default')
        
        print(f"[INFO] Preparando archivos en {remote_base_dir}")
        
        # Crear directorios necesarios
        directories_to_create = [
            f'{remote_base_dir}/share_data',
            f'{remote_base_dir}/share_data/input', 
            f'{remote_base_dir}/share_data/output',
            f'{remote_base_dir}/share_data/output/incendio{mission_id}'
        ]
        
        for directory in directories_to_create:
            try:
                sftp.mkdir(directory)
                print(f"[INFO] Directorio creado: {directory}")
            except Exception as e:
                print(f"[INFO] Directorio ya existe: {directory}")
        
        # Guardar configuración JSON para el algoritmo R
        with sftp.file(config_file, 'w') as remote_file:
            json.dump(config, remote_file, indent=4)
        print(f"[INFO] Configuración guardada: {config_file}")
        
        # Descargar imágenes reales desde MinIO (sin fallback)
        print("[INFO] Descargando imágenes desde MinIO...")
        
        # Obtener datos de ráfagas del frontend
        selected_bursts_data = input_data.get('selected_bursts', [])
        minio_base_url = input_data.get('minio_base_url', 'http://minio:9000')
        mission_id = config.get('fireId')
        
        for burst_data in selected_bursts_data:
            if isinstance(burst_data, dict):
                # Nueva estructura con imágenes específicas por ráfaga
                burst_id = burst_data.get('burst_id')
                burst_images = burst_data.get('selected_images', [])  #Imágenes de esta ráfaga
                timestamp = burst_data.get('timestamp')
                
                print(f"[INFO] Procesando ráfaga {burst_id} con {len(burst_images) if burst_images else 'todas las'} imágenes")
            else:
                # Estructura simple (fallback)
                burst_id = burst_data
                burst_images = []
                print(f"[INFO] Procesando ráfaga {burst_id} (todas las imágenes)")
            
            # Construir URL específica de MinIO para esta ráfaga
            minio_url = f"{minio_base_url}/missions/{mission_id}/burst{burst_id}/"
            remote_burst_dir = f'{remote_base_dir}/share_data/input/incendio{mission_id}/{burst_id}'
            
            # Crear directorio de ráfaga
            try:
                sftp.mkdir(f'{remote_base_dir}/share_data/input/incendio{mission_id}')
                sftp.mkdir(remote_burst_dir)
                print(f"[INFO] Directorio de ráfaga creado: {remote_burst_dir}")
            except:
                print(f"[INFO] Directorio ya existe: {remote_burst_dir}")
            
            # Descargar imágenes específicas de esta ráfaga
            if burst_images:
                # CASO: Usuario seleccionó imágenes específicas para esta ráfaga
                print(f"[INFO] Descargando {len(burst_images)} imágenes específicas para ráfaga {burst_id}")
                
                images_downloaded = 0
                for image_name in burst_images:
                    image_url = f"{minio_url.rstrip('/')}/{image_name}"
                    download_cmd = f'wget -q -O {remote_burst_dir}/{image_name} "{image_url}"'
                    
                    stdin, stdout, stderr = ssh_client.exec_command(download_cmd)
                    exit_status = stdout.channel.recv_exit_status()
                    
                    if exit_status == 0:
                        print(f"[INFO] Descargada: {image_name} para ráfaga {burst_id}")
                        images_downloaded += 1
                    else:
                        error_msg = stderr.read().decode()
                        print(f"[ERROR] Error descargando {image_name}: {error_msg}")
                
                if images_downloaded == 0:
                    raise Exception(f"No se pudo descargar ninguna imagen específica para ráfaga {burst_id}")
                    
            else:
                # CASO: Usuario quiere TODAS las imágenes de esta ráfaga
                print(f"[INFO] Descargando todas las imágenes para ráfaga {burst_id}")
                
                download_all_cmd = f'''
                curl -s "{minio_url.rstrip('/')}" | grep -o 'href="[^"]*\.tif"' | sed 's/href="//;s/"//' | while read file; do
                    wget -q -O {remote_burst_dir}/$file "{minio_url.rstrip('/')}/$file"
                    echo "Descargado: $file"
                done
                '''
                
                stdin, stdout, stderr = ssh_client.exec_command(download_all_cmd)
                exit_status = stdout.channel.recv_exit_status()
                download_output = stdout.read().decode()
                
                if exit_status == 0 and "Descargado:" in download_output:
                    print(f"[INFO] Todas las imágenes descargadas para ráfaga {burst_id}")
                else:
                    error_output = stderr.read().decode()
                    raise Exception(f"Error descargando todas las imágenes para ráfaga {burst_id}: {error_output}")
            
            # Verificar y actualizar imageCount para esta ráfaga específica
            files_check = sftp.listdir(remote_burst_dir)
            tif_files = [f for f in files_check if f.endswith('.tif')]
            
            if len(tif_files) > 0:
                print(f"[INFO] Se descargaron {len(tif_files)} imágenes para ráfaga {burst_id}")
                
                # Actualizar imageCount en la configuración para esta ráfaga específica
                for burst_config in config["infraredBursts"]:
                    if burst_config["id"] == f"PO{burst_id}":
                        burst_config["imageCount"] = len(tif_files)
                        break
            else:
                raise Exception(f"No se encontraron imágenes para ráfaga {burst_id}")
        
        print("[INFO] Descarga de imágenes desde MinIO completada")
        
        # Actualizar configuración con imageCount correcto
        with sftp.file(config_file, 'w') as remote_file:
            json.dump(config, remote_file, indent=4)
        print("[INFO] Configuración actualizada con conteo real de imágenes")
        
        # Crear archivo .env para Docker
        env_content = f"""VOLUME_PATH={remote_base_dir}/share_data
ALG_DIR=.
CONFIGURATION_PATH=/share_data/input/config_{job_id}.json
OUTDIR=/share_data/output/incendio{mission_id}
OUTPUT=true
CONTAINER_NAME=thermal_perimeter_{job_id}
"""
        with sftp.file(f'{launch_dir}/.env', 'w') as env_file:
            env_file.write(env_content)
        print("[INFO] Archivo .env creado")
        
        # Verificar que el archivo de configuración existe
        try:
            stat_info = sftp.stat(config_file)
            print(f"[INFO] Archivo de configuración verificado: {stat_info.st_size} bytes")
        except Exception as e:
            raise Exception(f"Error: no se pudo verificar el archivo de configuración: {e}")
        
        sftp.close()
        
        # Configurar permisos del script
        print("[INFO] Configurando permisos...")
        ssh_client.exec_command(f'chmod +x {remote_base_dir}/launch/install_geospatial.sh')
        time.sleep(1)
        
        # Limpiar contenedores anteriores
        print("[INFO] Limpiando contenedores anteriores...")
        cleanup_command = f'cd {launch_dir} && docker compose down --volumes'
        stdin, stdout, stderr = ssh_client.exec_command(cleanup_command)
        stdout.channel.recv_exit_status()
        time.sleep(2)
        
        # Ejecutar algoritmo
        print("[INFO] Ejecutando algoritmo de perímetros térmicos...")
        
        stdin, stdout, stderr = ssh_client.exec_command(
            f'cd {launch_dir} && docker compose up --build'
        )
        
        # Esperar a que termine y obtener resultados
        exit_status = stdout.channel.recv_exit_status()
        output = stdout.read().decode()
        error_output = stderr.read().decode()
        
        print("[INFO] SALIDA DEL ALGORITMO:")
        print("[INFO]", output)
        
        if error_output:
            print("[ERROR] ERRORES:")
            print("[ERROR]", error_output)
        
        # Verificar resultado
        if exit_status != 0:
            raise Exception(f"Docker falló con código {exit_status}: {error_output}")
        
        # Verificar códigos de estado del algoritmo R
        if "Status 1:" in output or 'No se han encontrado imágenes' in output:
            raise Exception("El algoritmo no encontró imágenes para procesar")
        elif "Status -100:" in output:
            raise Exception("Error desconocido en el algoritmo")
        elif "Status 0:" in output or "El algoritmo se ha ejecutado correctamente" in output:
            print("[INFO] Algoritmo completado exitosamente")
        
        # Verificar archivos de salida
        output_dir = f'{remote_base_dir}/share_data/output/incendio{mission_id}'
        expected_files = ['mosaico.tiff', 'output.json', 'perimetro.gpkg']
        
        print("[INFO] Verificando archivos de salida...")
        files_found = 0
        for file_name in expected_files:
            file_path = f'{output_dir}/{file_name}'
            try:
                sftp = ssh_client.open_sftp()
                file_stat = sftp.stat(file_path)
                print(f"[INFO] Archivo generado: {file_name} ({file_stat.st_size} bytes)")
                files_found += 1
            except FileNotFoundError:
                print(f"[ERROR] Archivo faltante: {file_name}")
            except Exception as e:
                print(f"[ERROR] Error verificando {file_name}: {str(e)}")
        
        if files_found == 0:
            raise Exception("No se generaron archivos de salida")
        else:
            print(f"[INFO] Se generaron {files_found}/{len(expected_files)} archivos esperados")
        
        sftp.close()
        print("[INFO] Algoritmo ejecutado correctamente")

# Función que cambia el estado del job a FINISHED cuando se completa el proceso
def change_job_status(**context):
    message = context['dag_run'].conf['message']
    job_id = message['id']
    update_job_status(job_id, 'FINISHED')

# Función que procesa los archivos generados, los sube a MinIO y los historiza en la base de datos
def post_process_and_historize(**context):
    
    # Extraer datos del contexto
    conf = context['dag_run'].conf
    message = conf.get("message", {})
    input_data = json.loads(message.get('input_data', '{}'))
    
    mission_id = input_data.get('mission_id')
    selected_bursts = input_data.get('selected_bursts', [])
    job_id = message.get('id')
    
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')
    
    with ssh_hook.get_conn() as ssh_client:
        sftp = ssh_client.open_sftp()
        
        # 1. DESCARGAR ARCHIVOS DEL SERVIDOR
        output_dir = f'/home/admin3/Algoritmo_deteccion_perimetro/share_data/output/incendio{mission_id}'
        local_temp_dir = f'/tmp/thermal_perimeter_{job_id}'
        os.makedirs(local_temp_dir, exist_ok=True)
        
        generated_files = []
        for file_name in ['mosaico.tiff', 'perimetro.gpkg', 'output.json']:
            try:
                remote_path = f'{output_dir}/{file_name}'
                local_path = f'{local_temp_dir}/{file_name}'
                sftp.get(remote_path, local_path)
                generated_files.append(file_name)
                print(f"[INFO] Archivo descargado: {file_name}")
            except Exception as e:
                print(f"[ERROR] Error descargando {file_name}: {str(e)}")
        
        sftp.close()
        
        # 2. SUBIR ARCHIVOS A MINIO
        minio_urls = {}
        try:
            s3_client = get_minio_client()
            
            for file_name in generated_files:
                local_file_path = f'{local_temp_dir}/{file_name}'
                minio_key = f"missions/{mission_id}/thermal_perimeter/{job_id}/{file_name}"
                
                # Subir archivo a MinIO
                s3_client.upload_file(local_file_path, 'results', minio_key)
                minio_urls[file_name] = f"minio://results/{minio_key}"
                print(f"[INFO] Archivo subido a MinIO: {file_name}")
                
        except Exception as e:
            print(f"[ERROR] Error subiendo a MinIO: {str(e)}")
            # Continuar aunque falle MinIO
        
       # 3. HISTORIZAR EN BASE DE DATOS
        try:
            madrid_tz = pytz.timezone('Europe/Madrid')
            
            # Input data siguiendo estructura estándar
            input_data_record = {
                "mission_id": mission_id,
                "selected_bursts": selected_bursts,
                "job_id": job_id,
                "advanced_params": input_data.get('advanced_params', {}),
                "selected_images": input_data.get('selected_images', []),
                "minio_base_url": input_data.get('minio_base_url', ''),
                "execution_time": datetime.now(madrid_tz).isoformat()
            }
            
            # Output data siguiendo estructura estándar
            output_data_record = {
                "generated_files": generated_files,
                "minio_urls": minio_urls,
                "execution_status": "FINISHED",
                "bursts_processed": len(selected_bursts),
                "files_count": len(generated_files),
                "remote_paths": [f'/home/admin3/Algoritmo_deteccion_perimetro/share_data/output/incendio{mission_id}/{f}' for f in generated_files]
            }
            
            datos = {
                "sampled_feature": mission_id, 
                "result_time": datetime.now(madrid_tz),
                "phenomenon_time": datetime.now(madrid_tz),
                "input_data": json.dumps(input_data_record),
                "output_data": json.dumps(output_data_record)
            }
            
            query = """
                INSERT INTO algoritmos.algoritmo_perimeter_detection (
                    sampled_feature, result_time, phenomenon_time, input_data, output_data
                ) VALUES (
                    :sampled_feature, :result_time, :phenomenon_time, :input_data, :output_data
                )
            """
            
            execute_query('biobd', query, datos)
            print("Datos del proceso guardados correctamente en la base de datos.")
            
        except Exception as e:
            print(f"Error al guardar en la base de datos: {str(e)}")
            # No fallar si la historización falla
        
        # 4. LIMPIAR ARCHIVOS TEMPORALES
        try:
            if os.path.exists(local_temp_dir):
                shutil.rmtree(local_temp_dir)
                print(f"Archivos temporales limpiados: {local_temp_dir}")
            else:
                print("No hay archivos temporales que limpiar")
        except Exception as e:
            print(f"Error limpiando archivos temporales: {str(e)}")
            # Continuar aunque falle la limpieza

# Configuración del DAG
default_args = {
    'owner': 'sergio',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# Definición del DAG principal
dag = DAG(
    'algorithm_thermal_perimeter_detection',
    default_args=default_args,
    description='DAG para generar perímetros de incendios a partir de imágenes termográficas',
    schedule_interval=None,  # Triggered by frontend/Kafka
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

# Tarea principal del algoritmo
execute_algorithm_task = PythonOperator(
    task_id='execute_thermal_perimeter_algorithm',
    python_callable=execute_thermal_perimeter_process,
    provide_context=True,
    dag=dag,
)

# Tarea de post-procesamiento e historización
post_process_task = PythonOperator(
    task_id='post_process_and_historize_results',
    python_callable=post_process_and_historize,
    provide_context=True,
    dag=dag,
)

# Cambiar estado a finalizado
change_status_task = PythonOperator(
    task_id='change_job_status_to_finished',
    python_callable=change_job_status,
    provide_context=True,
    dag=dag,
)

# FLUJO del DAG
execute_algorithm_task >> post_process_task >> change_status_task