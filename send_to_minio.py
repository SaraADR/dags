import boto3
import os

def upload_file_to_minio(file_path, bucket_name, object_name):
    # Conexión al cliente de MinIO
    minio_client = boto3.client('s3',
                                endpoint_url='http://minio:9000',
                                aws_access_key_id='YOUR_ACCESS_KEY',
                                aws_secret_access_key='YOUR_SECRET_KEY')

    # Subir el archivo
    minio_client.upload_file(file_path, bucket_name, object_name)

if __name__ == "__main__":
    file_path = '/app/data/sample.pdf'
    bucket_name = 'pdf-bucket'
    object_name = 'sample.pdf'
    upload_file_to_minio(file_path, bucket_name, object_name)
