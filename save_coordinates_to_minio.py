import docker
import json
import os
from minio import Minio
from minio.error import S3Error

# Initialize MinIO client
minio_client = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Docker client
client = docker.from_env()

def save_coordinates(data):
    # Save JSON to a file
    json_file_path = "/tmp/input.json"
    with open(json_file_path, "w") as json_file:
        json.dump(data, json_file)
    
    # Run Docker container to convert JSON to PDF
    container = client.containers.run(
        image="json_to_pdf_converter",
        volumes={json_file_path: {'bind': '/app/input.json', 'mode': 'rw'},
                 "/tmp/output.pdf": {'bind': '/app/output.pdf', 'mode': 'rw'}},
        detach=True
    )
    
    # Wait for the container to finish
    container.wait()
    
    # Check if the PDF was created
    pdf_file_path = "/tmp/output.pdf"
    if os.path.exists(pdf_file_path):
        # Upload PDF to MinIO
        bucket_name = "pdfs"
        object_name = "output.pdf"
        minio_client.fput_object(bucket_name, object_name, pdf_file_path)
        print(f"PDF successfully uploaded to MinIO as {bucket_name}/{object_name}")
    else:
        print("PDF was not created")

# Example usage
if __name__ == "__main__":
    # Load test data
    with open("testfile.json", "r") as file:
        data = json.load(file)
    
    # Save coordinates
    save_coordinates(data)
