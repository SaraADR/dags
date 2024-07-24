import json
import docker
import minio
from minio import Minio
from minio.error import S3Error
import os

def save_coordinates(data):
    # Convertir JSON a PDF usando Docker
    client = docker.from_env()
    
    # Crear archivos temporales para input y output
    input_file = 'data.json'
    output_dir = 'output'
    output_file = os.path.join(output_dir, 'data.pdf')

    with open(input_file, 'w') as f:
        json.dump(data, f)

    os.makedirs(output_dir, exist_ok=True)

    try:
        client.containers.run(
            image="json_to_pdf_converter",  # Nombre de la imagen de Docker
            volumes={
                os.path.abspath(input_file): {'bind': '/input/data.json', 'mode': 'ro'},
                os.path.abspath(output_dir): {'bind': '/output', 'mode': 'rw'}
            },
            remove=True
        )
    except docker.errors.ContainerError as e:
        print(f"Error al ejecutar el contenedor Docker: {e}")
        return

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
            "locationtest",  # Nombre del bucket
            "data.pdf",     # Nombre del archivo en MinIO
            output_file     # Ruta del archivo PDF generado
        )
        print(f"PDF subido exitosamente a MinIO. ETag: {result.etag}")
    except S3Error as e:
        print(f"Fallo al subir el PDF a MinIO: {e}")

# Ejemplo de uso
if __name__ == "__main__":
    with open('testfile.json', 'r') as f:
        data = json.load(f)
    save_coordinates(data)
