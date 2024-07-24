import json
import docker
import minio
from minio import Minio
from minio.error import S3Error
import os

def save_coordinates(data):
    # Convertir JSON a PDF usando Docker
    client = docker.from_env()
    with open('data.json', 'w') as f:
        json.dump(data, f)
    
    os.makedirs('output', exist_ok=True)
    
    client.containers.run(
        image="json_to_pdf_converter",  # Nombre de la imagen de Docker
        volumes={
            os.path.abspath('data.json'): {'bind': '/input/data.json', 'mode': 'ro'},
            os.path.abspath('output'): {'bind': '/output', 'mode': 'rw'}
        },
        remove=True
    )

    # Configuración del cliente de MinIO
    minio_client = Minio(
        "minio:9000",
        access_key="YOUR-ACCESSKEYID",
        secret_key="YOUR-SECRETACCESSKEY",
        secure=False
    )

    # Subir PDF a MinIO
    try:
        result = minio_client.fput_object(
            "bucket-name",  # Nombre del bucket
            "data.pdf",     # Nombre del archivo en MinIO
            "output/data.pdf" # Ruta del archivo PDF generado
        )
        print(f"PDF subido exitosamente a MinIO. ETag: {result.etag}")
    except S3Error as e:
        print(f"Fallo al subir el PDF a MinIO: {e}")

# Ejemplo de uso
if __name__ == "__main__":
    with open('testfile.json', 'r') as f:
        data = json.load(f)
    save_coordinates(data)
