import os
import subprocess

def build_docker_image():
    # Construir la imagen Docker
    subprocess.run(["docker", "build", "-t", "pdf-to-minio", "."], check=True)

def run_docker_container(pdf_path):
    # Crear el directorio /app/data en el contenedor y copiar el PDF allí
    subprocess.run(["docker", "run", "--rm", 
                    "-v", f"{os.path.abspath(pdf_path)}:/app/data/sample.pdf", 
                    "pdf-to-minio"], check=True)

if __name__ == "__main__":
    pdf_path = 'path/to/your/pdf/sample.pdf'

    # Construir la imagen Docker
    build_docker_image()

    # Ejecutar el contenedor Docker y enviar el PDF a MinIO
    run_docker_container(pdf_path)
