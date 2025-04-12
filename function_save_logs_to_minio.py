# function_save_logs_to_minio.py
import os
from datetime import datetime
from botocore.exceptions import ClientError
from dag_utils import get_minio_client

def save_logs_to_minio(**context):
    """
    Guarda los logs de la ejecución actual en Minio
    """
    try:
        # Obtener información del contexto
        dag_id = context['dag'].dag_id
        run_id = context['run_id']
        task_id = 'consume_from_topic_minio'
        execution_date = context['execution_date']
        
        # Formato de fecha requerido: 20250325T093838
        formatted_date = execution_date.strftime('%Y%m%dT%H%M%S')
        
        # Ruta base de logs
        log_base_folder = "/opt/airflow/logs"
        print(f"Base de logs: {log_base_folder}")
        
        # Construir la ruta basada en la estructura observada
        log_dir_path = f"{log_base_folder}/dag_id={dag_id}/run_id={run_id}"
        print(f"Buscando directorio de logs en: {log_dir_path}")
        
        if not os.path.exists(log_dir_path):
            print(f"El directorio de logs no existe: {log_dir_path}")
            # Intentar listar el directorio padre para ver qué hay disponible
            parent_dir = f"{log_base_folder}/dag_id={dag_id}"
            if os.path.exists(parent_dir):
                print(f"Contenido del directorio del DAG: {os.listdir(parent_dir)}")
            return
        
        # Buscar el archivo de logs del task específico
        task_logs = []
        for root, dirs, files in os.walk(log_dir_path):
            for file in files:
                if task_id in file or task_id in root:
                    full_path = os.path.join(root, file)
                    task_logs.append(full_path)
                    print(f"Archivo de log encontrado: {full_path}")
        
        if not task_logs:
            print(f"No se encontraron logs para la tarea {task_id} en {log_dir_path}")
            # Mostrar todos los archivos disponibles
            print("Archivos disponibles:")
            for root, dirs, files in os.walk(log_dir_path):
                for file in files:
                    print(os.path.join(root, file))
            return
        
        # Usar el primer archivo encontrado (o podrías concatenarlos todos)
        log_path = task_logs[0]
        
        # Leer el contenido del log
        with open(log_path, 'r') as log_file:
            log_content = log_file.read()
        
        print(f"Contenido del log leído correctamente, tamaño: {len(log_content)} bytes")
        
        # Establecer conexión con MinIO
        s3_client = get_minio_client()
        
        # Guardar en Minio con el nuevo formato de ruta
        bucket_name = 'logs'
        key = f"airflow/{formatted_date}-{dag_id}.txt"
        
        # Comprobar si existe el bucket, si no, crearlo
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except ClientError:
            print(f"Creando bucket: {bucket_name}")
            s3_client.create_bucket(Bucket=bucket_name)
        
        # Guardar el log
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=log_content
        )
        
        print(f"Log guardado con éxito en Minio: bucket={bucket_name}, key={key}")
    except Exception as e:
        import traceback
        print(f"Error al guardar logs en Minio: {str(e)}")
        traceback.print_exc()