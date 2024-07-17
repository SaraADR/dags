from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from minio import Minio
from minio.error import S3Error
import json
import subprocess

# Default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'process_and_upload_dag',
    default_args=default_args,
    description='A simple DAG to process coordinates and upload PDF to MinIO',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Function to read coordinates from file
def read_coordinates(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

# Function to call Docker container and generate the PDF
def generate_pdf(coordinates):
    command = f"docker exec e28a773e8a67bedd2a5006965e2bb6afc88f49d652bcfab0dfc1b77b5b3dd191 generate_pdf_script.py --coords '{json.dumps(coordinates)}'"
    subprocess.run(command, shell=True, check=True)

# Function to upload the PDF to MinIO and get the ID
def upload_to_minio(pdf_path, bucket_name, minio_client):
    try:
        pdf_id = pdf_path.split('/')[-1]
        minio_client.fput_object(
            bucket_name, pdf_id, pdf_path,
        )
        return pdf_id
    except S3Error as exc:
        print("error occurred.", exc)
        return None

# Main function to process coordinates and upload PDF
def process_and_upload(**kwargs):
    # Path to the coordinates file (ensure this path is accessible)
    coordinates_file = "/path/to/inputs.json"

    # Read the coordinates
    coordinates = read_coordinates(coordinates_file)
    
    # Generate the PDF
    generate_pdf(coordinates)
    
    # MinIO client configuration
    minio_client = Minio(
        "play.min.io",  # Replace with your MinIO endpoint
        access_key="xsytzGmjdOucIZrhUa7G",  # Replace with your access key
        secret_key="JBRFy79EJajDNLiZzkehJz9rY7wSHcruHgWTcz7M",  # Replace with your secret key
        secure=True
    )

    # Path to the generated PDF (ensure this matches the Docker script output)
    pdf_path = "/path/to/generated_pdf.pdf"
    
    # MinIO bucket name
    bucket_name = "locationtest"

    # Upload the PDF to MinIO and get the ID
    pdf_id = upload_to_minio(pdf_path, bucket_name, minio_client)

    if pdf_id:
        print(f"PDF uploaded successfully. MinIO ID: {pdf_id}")
    else:
        print("Failed to upload PDF to MinIO.")

# Define the task
process_and_upload_task = PythonOperator(
    task_id='process_and_upload',
    python_callable=process_and_upload,
    provide_context=True,
    dag=dag,
)

# Set the task in the DAG
process_and_upload_task
