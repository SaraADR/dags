# process_and_upload.py

import os
import subprocess
import boto3

def convert_to_pdf(input_file, output_file):
    subprocess.run(["docker-compose", "run", "--rm", "pdf_converter", input_file, output_file], check=True)

def upload_to_minio(output_file, bucket_name, object_name):
    minio_url = 'http://localhost:9000'
    access_key = 'minioadmin'
    secret_key = 'minioadmin'

    minio_client = boto3.client('s3', endpoint_url=minio_url, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    minio_client.upload_file(output_file, bucket_name, object_name)

if __name__ == "__main__":
    input_file = '/input/process_coordinates.py'
    output_file = '/output/process_coordinates.pdf'
    bucket_name = 'my-bucket'
    object_name = 'process_coordinates.pdf'

    # Crear directorios si no existen
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    convert_to_pdf(input_file, output_file)
    upload_to_minio(output_file, bucket_name, object_name)
