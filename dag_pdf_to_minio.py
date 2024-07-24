from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 24),
    'retries': 1,
}

dag = DAG(
    'pdf_to_minio',
    default_args=default_args,
    description='Enviar un PDF a MinIO usando Docker',
    schedule_interval=None,
)

def build_docker_image():
    subprocess.run(["docker", "build", "-t", "pdf-to-minio", "/path/to/your/Dockerfile_directory"], check=True)

build_image = PythonOperator(
    task_id='build_docker_image',
    python_callable=build_docker_image,
    dag=dag,
)

run_docker_container = DockerOperator(
    task_id='run_docker_container',
    image='pdf-to-minio',
    api_version='auto',
    auto_remove=True,
    volumes=[f"{os.path.abspath('path/to/your/pdf/sample.pdf')}:/app/data/sample.pdf"],
    command="python /app/send_to_minio.py",
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    dag=dag,
)

build_image >> run_docker_container
