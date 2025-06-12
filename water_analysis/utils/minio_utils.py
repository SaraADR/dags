from botocore.exceptions import ClientError
import os
import shutil
from typing import Optional, List

def download_from_minio(s3_client, bucket_name, file_path_in_minio, local_directory, folder_prefix):
    """
    Función para descargar archivos o carpetas desde MinIO.
    """
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    files = list_files_in_minio_folder(s3_client, bucket_name, folder_prefix)
    if not files:
        print(f"No se encontraron archivos para descargar en la carpeta: {folder_prefix}")
        return

    local_file = os.path.join(local_directory, os.path.basename(file_path_in_minio))
    print(f"Descargando archivo desde MinIO: {file_path_in_minio} a {local_file}")
    
    relative_path = file_path_in_minio.replace('/tmp/', '')
    try:
        # Verificar si el archivo existe antes de intentar descargarlo
        response = s3_client.get_object(Bucket=bucket_name, Key=relative_path)
        with open(local_file, 'wb') as f:
            shutil.copyfileobj(response['Body'], f, length=16*1024)

        print(f"Archivo descargado correctamente: {local_file}")

        return local_file
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Error 404: El archivo no fue encontrado en MinIO: {file_path_in_minio}")
        else:
            print(f"Error en el proceso: {str(e)}")
        return None  # Devolver None si hay un error

def list_files_in_minio_folder(s3_client, bucket_name, prefix):
    """
    Lista todos los archivos dentro de un prefijo (directorio) en MinIO.
    """

    print(bucket_name),
    print(prefix)
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in response:
            print(f"No se encontraron archivos en la carpeta: {prefix}")
            return []

        files = [content['Key'] for content in response['Contents']]
        return files

    except ClientError as e:
        print(f"Error al listar archivos en MinIO: {str(e)}")
        return []