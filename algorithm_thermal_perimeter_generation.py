import json
import os
import time
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
from dag_utils import throw_job_error, update_job_status
from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook

# Función que ejecuta el algoritmo de generación de perímetros térmicos utilizando los datos recibidos desde la interfaz y ejecuta el Docker.
def execute_thermal_perimeter_process(**context):

    conf = context.get("dag_run").conf
    if not conf:
        print("Error: No se recibió configuración desde el DAG.")
        return
    
    print("Datos recibidos del DAG:")
    print(json.dumps(conf, indent=4))

    message = conf.get("message", {})
    trace_id = conf.get("trace_id", "no-trace")
    print(f"Processing with trace_id: {trace_id}")
    
    job_id = message.get('id')
    input_data_str = message.get('input_data', '{}')
    
    try:
        input_data = json.loads(input_data_str)
    except json.JSONDecodeError:
        print("Error al decodificar 'input_data'")
        throw_job_error(job_id, "Invalid JSON in input_data")
        return

    # Extraer datos básicos de la interfaz
    mission_id = input_data.get('mission_id')
    selected_bursts = input_data.get('selected_bursts', [])
    selected_images = input_data.get('selected_images', [])
    advanced_params = input_data.get('advanced_params', {})
    
    if not mission_id:
        throw_job_error(job_id, "No se especificó ID de misión")
        return
    
    if not selected_bursts:
        throw_job_error(job_id, "No se seleccionaron ráfagas")
        return

    print("Datos extraídos correctamente:")
    print(f"Mission ID: {mission_id}")
    print(f"Selected bursts: {selected_bursts}")
    print(f"Selected images: {len(selected_images) if selected_images else 'todas'}")
    print(f"Advanced params: {advanced_params}")
    
    # Crear configuración básica para el algoritmo
    algorithm_config = create_basic_config(mission_id, selected_bursts, advanced_params, job_id)
    
    # Ejecutar Docker (pasar también los datos originales para acceso a URLs)
    try:
        execute_docker_algorithm(algorithm_config, job_id, input_data)
        print("Algoritmo Docker ejecutado correctamente")
    except Exception as e:
        print(f"Error ejecutando algoritmo: {str(e)}")
        throw_job_error(job_id, str(e))
        raise

# Crea la configuración básica para el algoritmo R según los parámetros recibidos.
def create_basic_config(mission_id, selected_bursts, advanced_params, job_id):
    
    # Parámetros avanzados con valores por defecto
    n_clusters = advanced_params.get('numberOfClusters', 3)
    threshold_temp = advanced_params.get('tresholdTemp', 2)
    
    # Configuración según especificación del algoritmo
    config = {
        "fireId": mission_id,
        "criteria": {
            "numberOfClusters": n_clusters,
            "tresholdTemp": threshold_temp
        },
        "infraredBursts": [
            {
                "path": f"/share_data/input/incendio{mission_id}/{burst_id}",
                "id": f"PO{burst_id}",
                "timestamp": "2024-07-16T12:00:00Z",
                "sensorId": "ThermalSensor",
                "imageCount": 5
            }
            for burst_id in selected_bursts
        ]
    }
    
    return config

# Ejecuta el algoritmo de perímetros térmicos usando Docker en el servidor remoto.
def execute_docker_algorithm(config, job_id, input_data):
    
    print("Iniciando ejecución Docker...")
    
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')
    
    with ssh_hook.get_conn() as ssh_client:
        sftp = ssh_client.open_sftp()
        
        # Rutas en el servidor remoto
        remote_base_dir = '/home/admin3/Algoritmo_deteccion_perimetro'
        config_file = f'{remote_base_dir}/share_data/input/config_{job_id}.json'
        launch_dir = f'{remote_base_dir}/launch'
        
        # Extraer mission_id del config para el directorio de salida
        mission_id = config.get('fireId', 'default')
        
        print(f"Preparando archivos en {remote_base_dir}")
        
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
                print(f"Directorio creado: {directory}")
            except Exception as e:
                print(f"Directorio ya existe: {directory}")
        
        # Guardar configuración JSON para el algoritmo R
        with sftp.file(config_file, 'w') as remote_file:
            json.dump(config, remote_file, indent=4)
        print(f"Configuración guardada: {config_file}")
        
        # Descargar imágenes reales desde MinIO (sin fallback)
        print("Descargando imágenes desde MinIO...")
        
        # Obtener datos de ráfagas del frontend
        selected_bursts_data = input_data.get('selected_bursts', [])
        
        for burst_data in selected_bursts_data:
            # Verificar si es estructura simple o completa
            if isinstance(burst_data, dict):
                burst_id = burst_data.get('id')
                images_source = burst_data.get('images_source', '')
                images_list = burst_data.get('images', [])
            else:
                # Estructura simple: solo números de ráfaga
                burst_id = burst_data
                # Construir URL base de MinIO (ajustar según tu configuración real)
                minio_base_url = input_data.get('minio_base_url', 'http://tu-minio-url')
                images_source = f"{minio_base_url}/missions/{mission_id}/burst{burst_id}/"
                images_list = []  # Se descargarán todas las disponibles
            
            remote_burst_dir = f'{remote_base_dir}/share_data/input/incendio{mission_id}/{burst_id}'
            
            try:
                sftp.mkdir(f'{remote_base_dir}/share_data/input/incendio{mission_id}')
                sftp.mkdir(remote_burst_dir)
                print(f"Directorio de ráfaga creado: {remote_burst_dir}")
            except:
                print(f"Directorio ya existe: {remote_burst_dir}")
            
            # Descargar imágenes desde MinIO
            if images_list:
                # Descargar imágenes específicas
                print(f"Descargando {len(images_list)} imágenes específicas para ráfaga {burst_id}")
                images_downloaded = 0
                
                for image_name in images_list:
                    image_url = f"{images_source.rstrip('/')}/{image_name}"
                    download_cmd = f'wget -q -O {remote_burst_dir}/{image_name} "{image_url}"'
                    
                    stdin, stdout, stderr = ssh_client.exec_command(download_cmd)
                    exit_status = stdout.channel.recv_exit_status()
                    
                    if exit_status == 0:
                        print(f"  ✓ Descargada: {image_name}")
                        images_downloaded += 1
                    else:
                        error_msg = stderr.read().decode()
                        print(f"  ✗ Error descargando {image_name}: {error_msg}")
                
                if images_downloaded == 0:
                    raise Exception(f"No se pudo descargar ninguna imagen para ráfaga {burst_id} desde {images_source}")
                
            else:
                # Descargar todas las imágenes .tif disponibles desde MinIO
                print(f"Descargando todas las imágenes .tif para ráfaga {burst_id} desde {images_source}")
                
                # Comando para listar y descargar archivos .tif desde MinIO
                download_all_cmd = f'''
                curl -s "{images_source.rstrip('/')}" | grep -o 'href="[^"]*\.tif"' | sed 's/href="//;s/"//' | while read file; do
                    wget -q -O {remote_burst_dir}/$file "{images_source.rstrip('/')}/$file"
                    echo "Descargado: $file"
                done
                '''
                
                stdin, stdout, stderr = ssh_client.exec_command(download_all_cmd)
                exit_status = stdout.channel.recv_exit_status()
                download_output = stdout.read().decode()
                
                if exit_status == 0 and "Descargado:" in download_output:
                    print(f"Imágenes descargadas desde MinIO para ráfaga {burst_id}")
                    print(f"Output: {download_output}")
                else:
                    error_output = stderr.read().decode()
                    raise Exception(f"Error descargando desde MinIO para ráfaga {burst_id}: {error_output}")
            
            # Verificar que se descargaron imágenes
            files_check = sftp.listdir(remote_burst_dir)
            tif_files = [f for f in files_check if f.endswith('.tif')]
            
            if len(tif_files) > 0:
                print(f"Se descargaron {len(tif_files)} imágenes para ráfaga {burst_id}")
                
                # Actualizar imageCount en la configuración
                for burst_config in config["infraredBursts"]:
                    if burst_config["id"] == f"PO{burst_id}":
                        burst_config["imageCount"] = len(tif_files)
                        break
            else:
                raise Exception(f"No se encontraron imágenes .tif después de la descarga para ráfaga {burst_id}. Verifique que las imágenes existan en MinIO: {images_source}")
        
        print("Descarga de imágenes desde MinIO completada")
        
        # Actualizar configuración con imageCount correcto
        with sftp.file(config_file, 'w') as remote_file:
            json.dump(config, remote_file, indent=4)
        print("Configuración actualizada con conteo real de imágenes")
        
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
        print("Archivo .env creado")
        
        # Verificar que el archivo de configuración existe
        try:
            stat_info = sftp.stat(config_file)
            print(f"✓ Archivo de configuración verificado: {stat_info.st_size} bytes")
        except Exception as e:
            raise Exception(f"Error: no se pudo verificar el archivo de configuración: {e}")
        
        sftp.close()
        
        # Configurar permisos del script
        print("Configurando permisos...")
        ssh_client.exec_command(f'chmod +x {remote_base_dir}/launch/install_geospatial.sh')
        time.sleep(1)
        
        # Limpiar contenedores anteriores
        print("Limpiando contenedores anteriores...")
        cleanup_command = f'cd {launch_dir} && docker compose down --volumes'
        stdin, stdout, stderr = ssh_client.exec_command(cleanup_command)
        stdout.channel.recv_exit_status()
        time.sleep(2)
        
        # Ejecutar algoritmo
        print("Ejecutando algoritmo de perímetros térmicos...")
        
        stdin, stdout, stderr = ssh_client.exec_command(
            f'cd {launch_dir} && docker compose up --build'
        )
        
        # Esperar a que termine y obtener resultados
        exit_status = stdout.channel.recv_exit_status()
        output = stdout.read().decode()
        error_output = stderr.read().decode()
        
        print("SALIDA DEL ALGORITMO:")
        print(output)
        
        if error_output:
            print("ERRORES:")
            print(error_output)
        
        # Verificar resultado
        if exit_status != 0:
            raise Exception(f"Docker falló con código {exit_status}: {error_output}")
        
        # Verificar códigos de estado del algoritmo R
        if "Status 1:" in output or 'No se han encontrado imágenes' in output:
            raise Exception("El algoritmo no encontró imágenes para procesar")
        elif "Status -100:" in output:
            raise Exception("Error desconocido en el algoritmo")
        elif "Status 0:" in output or "El algoritmo se ha ejecutado correctamente" in output:
            print("✓ Algoritmo completado exitosamente")
        
        # Verificar archivos de salida
        output_dir = f'{remote_base_dir}/share_data/output/incendio{mission_id}'
        expected_files = ['mosaico.tiff', 'output.json', 'perimetro.gpkg']
        
        print("Verificando archivos de salida...")
        files_found = 0
        for file_name in expected_files:
            file_path = f'{output_dir}/{file_name}'
            try:
                sftp = ssh_client.open_sftp()
                file_stat = sftp.stat(file_path)
                print(f"✓ Archivo generado: {file_name} ({file_stat.st_size} bytes)")
                files_found += 1
            except FileNotFoundError:
                print(f"✗ Archivo faltante: {file_name}")
            except Exception as e:
                print(f"? Error verificando {file_name}: {str(e)}")
        
        if files_found == 0:
            raise Exception("No se generaron archivos de salida")
        else:
            print(f"✓ Se generaron {files_found}/{len(expected_files)} archivos esperados")
        
        sftp.close()
        print("Algoritmo ejecutado correctamente")

# Función que cambia el estado del job a FINISHED cuando se completa el proceso
def change_job_status(**context):
    message = context['dag_run'].conf['message']
    job_id = message['id']
    update_job_status(job_id, 'FINISHED')

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
    'algorithm_thermal_perimeter_generation',
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

# Cambiar estado a finalizado
change_status_task = PythonOperator(
    task_id='change_job_status_to_finished',
    python_callable=change_job_status,
    provide_context=True,
    dag=dag,
)

# Flujo del DAG
execute_algorithm_task >> change_status_task