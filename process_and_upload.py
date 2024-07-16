import json
import docker
from minio import Minio
from minio.error import S3Error

# Initialize minio client
minio_client = Minio(
    "play.min.io",
    access_key="admin",
    secret_key="HvQAS5ZiSY",
    secure=True
)

def save_to_minio(bucket_name, object_name, data):
    try:
        # Make bucket if not exist
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
        # Save data to minio
        minio_client.put_object(bucket_name, object_name, data, len(data))
        print(f"Successfully uploaded {object_name} to {bucket_name}")
    except S3Error as e:
        print(f"Error occurred: {e}")

def send_to_docker(data):
    client = docker.from_env()
    container = client.containers.run('your_docker_image', data, detach=True)
    response = container.logs()
    container.remove()
    return response

def process_message(message):
    required_content = {
        "boundingBox": {
            "x_min": 604592.0659992056,
            "x_max": 604744.9679992045,
            "y_min": 4742773.29511746,
            "y_max": 4742861.794117461,
            "z_min": 677.375,
            "z_max": 702.05
        },
        "annotation": {
            "x": 604604.944,
            "y": 4742852.381,
            "z": 1.7210693359375,
            "title": "Anotación usuario"
        }
    }

    if message == required_content:
        response = send_to_docker(json.dumps(message))
        save_to_minio("my_bucket", "response.json", response)
    else:
        print("Message content is not as expected")

# Example usage
if __name__ == "__main__":
    consumer_message = {
        "boundingBox": {
            "x_min": 604592.0659992056,
            "x_max": 604744.9679992045,
            "y_min": 4742773.29511746,
            "y_max": 4742861.794117461,
            "z_min": 677.375,
            "z_max": 702.05
        },
        "annotation": {
            "x": 604604.944,
            "y": 4742852.381,
            "z": 1.7210693359375,
            "title": "Anotación usuario"
        }
    }

    process_message(consumer_message)
