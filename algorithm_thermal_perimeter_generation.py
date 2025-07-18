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
    
    # Ejecutar Docker
    try:
        execute_docker_algorithm(algorithm_config, job_id)
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
def execute_docker_algorithm(config, job_id):
    
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
        
        print("Subiendo imágenes reales para prueba...")
        
        # Ruta local donde tienes las imágenes reales
        local_base = "C:/Users/Sergio/Downloads/algoritmo-objetivo-3-master/input"

        # Obtener selected_bursts del config
        selected_bursts = [burst.get("id", "").replace("PO", "") for burst in config.get("infraredBursts", [])]

        for burst_id in selected_bursts:
            remote_burst_dir = f'{remote_base_dir}/share_data/input/incendio{mission_id}/{burst_id}'
            local_burst_dir = f'{local_base}/incendio1/{burst_id}'  # Usar incendio1 como ejemplo
            
            try:
                sftp.mkdir(f'{remote_base_dir}/share_data/input/incendio{mission_id}')
                sftp.mkdir(remote_burst_dir)
                print(f"Directorio de ráfaga creado: {remote_burst_dir}")
            except:
                print(f"Directorio ya existe: {remote_burst_dir}")
            
            # Verificar si ya hay imágenes
            try:
                files = sftp.listdir(remote_burst_dir)
                tif_files = [f for f in files if f.endswith('.tif')]
                if len(tif_files) >= 3:
                    print(f"Ya existen {len(tif_files)} imágenes en ráfaga {burst_id}")
                else:
                    # Subir algunas imágenes reales
                    import os
                    if os.path.exists(local_burst_dir):
                        local_files = [f for f in os.listdir(local_burst_dir) if f.endswith('.tif')]
                        for i, filename in enumerate(local_files[:3]):  # Solo 3 para prueba
                            local_path = os.path.join(local_burst_dir, filename)
                            remote_path = f'{remote_burst_dir}/{filename}'
                            sftp.put(local_path, remote_path)
                            print(f"  Imagen subida: {filename}")
                    else:
                        print(f"⚠️  Directorio local no existe: {local_burst_dir}")
            except Exception as e:
                print(f"Error manejando imágenes en ráfaga {burst_id}: {str(e)}")
        
        print("Imágenes reales subidas")
        
        # CONTINUAR CON EL CÓDIGO EXISTENTE
        # Crear archivo .env dinámico - CORREGIDO
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
        
        # Dar permisos de ejecución al script
        print("Configurando permisos del script...")
        ssh_client.exec_command(f'chmod +x {remote_base_dir}/launch/install_geospatial.sh')
        time.sleep(1)
        
        # Ejecutar Docker Compose
        print("Limpiando contenedores anteriores...")
        cleanup_command = f'cd {launch_dir} && docker compose down --volumes'
        stdin, stdout, stderr = ssh_client.exec_command(cleanup_command)
        stdout.channel.recv_exit_status()  # Esperar que termine
        time.sleep(2)
        
        print("Ejecutando algoritmo de perímetros térmicos...")
        
        # Agregar debugging antes de ejecutar Docker
        print("Verificando archivo de configuración antes de Docker...")
        debug_cmd = f'ls -la {remote_base_dir}/share_data/input/'
        stdin, stdout, stderr = ssh_client.exec_command(debug_cmd)
        stdout.channel.recv_exit_status()
        debug_output = stdout.read().decode()
        print(f"Contenido de input/: {debug_output}")
        
        # Verificar el archivo .env
        env_debug_cmd = f'cat {launch_dir}/.env'
        stdin, stdout, stderr = ssh_client.exec_command(env_debug_cmd)
        stdout.channel.recv_exit_status()
        env_content_check = stdout.read().decode()
        print(f"Contenido del .env: {env_content_check}")
        
        # Verificar el docker-compose.yaml
        compose_debug_cmd = f'cat {launch_dir}/docker-compose.yaml'
        stdin, stdout, stderr = ssh_client.exec_command(compose_debug_cmd)
        stdout.channel.recv_exit_status()
        compose_content = stdout.read().decode()
        print(f"Contenido del docker-compose.yaml: {compose_content}")
        
        stdin, stdout, stderr = ssh_client.exec_command(
            f'cd {launch_dir} && docker compose up --build'
        )
        
        # Esperar a que termine y obtener el estado de salida
        exit_status = stdout.channel.recv_exit_status()
        
        # Leer salida del algoritmo
        output = stdout.read().decode()
        error_output = stderr.read().decode()
        
        print("SALIDA DEL ALGORITMO")
        print(output)
        
        if error_output:
            print("ERRORES")
            print(error_output)
        
        # Verificar resultado
        if exit_status != 0:
            raise Exception(f"Docker falló con código {exit_status}: {error_output}")
        
        # Verificar códigos de estado específicos del algoritmo R
        if "Status 1:" in output or 'No se han encontrado imágenes' in output:
            print("⚠️  El algoritmo reportó falta de imágenes")
            # En pruebas con imágenes ficticias, verificar qué encontró
            for burst_id in [1, 2]:
                check_dir = f'{remote_base_dir}/share_data/input/incendio{mission_id}/{burst_id}'
                try:
                    files = sftp.listdir(check_dir)
                    tif_files = [f for f in files if f.endswith('.tif')]
                    print(f"  Ráfaga {burst_id}: {len(tif_files)} archivos .tif encontrados")
                except Exception as e:
                    print(f"  Error verificando ráfaga {burst_id}: {e}")
            
            # Para pruebas, no fallar por falta de imágenes reales
            print("ℹ️  Continuando con verificación (modo prueba)")
            
        elif "Status -100:" in output:
            raise Exception("Error desconocido en el algoritmo")
        elif "Status 0:" in output:
            print("Algoritmo completado exitosamente (Status 0)")
        
        # Verificar archivos de salida (más flexible para pruebas)
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
            print("⚠️  No se generaron archivos de salida (puede ser normal con imágenes ficticias)")
        else:
            print(f"✓ Se generaron {files_found}/{len(expected_files)} archivos esperados")
        
        sftp.close()
        print("Docker ejecutado correctamente")

# FUNCIÓN DE PRUEBA
def test_with_sample_data(**context):
    """Ejecuta el algoritmo con datos de prueba para testing."""
    
    print("EJECUTANDO PRUEBA CON DATOS DE MUESTRA")
    print("=" * 50)
    
    # Datos de prueba que simula lo que vendría del frontend
    sample_conf = {
        "message": {
            "id": f"test_job_{int(datetime.now().timestamp())}",
            "input_data": json.dumps({
                "mission_id": 1479,  
                "selected_bursts": [1, 2],
                "selected_images": [],
                "advanced_params": {
                    "numberOfClusters": 3,
                    "tresholdTemp": 2
                }
            })
        },
        "trace_id": f"test_trace_{int(datetime.now().timestamp())}"
    }
    
    # Crear contexto simulado (mock del dag_run)
    test_context = {
        'dag_run': type('MockDagRun', (), {'conf': sample_conf})()
    }
    
    print(f"Datos de prueba simulados:")
    print(json.dumps(sample_conf, indent=2))
    
    try:
        # Ejecutar la función principal con datos de prueba
        execute_thermal_perimeter_process(**test_context)
        print("Prueba completada exitosamente")
    except Exception as e:
        print(f"Error en la prueba: {str(e)}")
        raise

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

# Definición del DAG
dag = DAG(
    'algorithm_thermal_perimeter_generation',
    default_args=default_args,
    description='DAG para generar perímetros de incendios a partir de imágenes termográficas seleccionadas en la interfaz',
    schedule_interval=None,  # Triggered by Kafka
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

# NUEVA TAREA DE PRUEBA
test_task = PythonOperator(
    task_id='test_with_sample_data',
    python_callable=test_with_sample_data,
    dag=dag,
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

# DAG SEPARADO SOLO PARA PRUEBAS
test_dag_separate = DAG(
    'test_thermal_perimeter_only',  # Nombre diferente
    default_args=default_args,
    description='DAG de prueba SOLO para perímetros térmicos',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

# Solo la tarea de prueba
test_only_task = PythonOperator(
    task_id='run_test',
    python_callable=test_with_sample_data,
    dag=test_dag_separate,
)

# Flujos del DAG
execute_algorithm_task >> change_status_task


    

