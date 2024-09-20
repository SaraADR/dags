from collections import defaultdict
from datetime import datetime, timedelta
import re
from airflow import DAG
import requests
from requests.auth import HTTPBasicAuth
from airflow.operators.python import PythonOperator

# Function to create an import context for GeoTIFF upload
def create_geotiff_import(workspace, geoserver_url, geoserver_user, geoserver_password, tiff_file_path):
    import_url = f"{geoserver_url}/rest/imports"
    headers = {
        'Content-type': 'application/json'
    }
    
    # JSON payload to create the import session
    data = {
        "import": {
            "targetWorkspace": {
                "workspace": {
                    "name": workspace
                }
            },
            "data": {
                "type": "file",
                "file": tiff_file_path
            }
        }
    }
    
    try:
        # Step 1: Create an import session
        response = requests.post(import_url, headers=headers, json=data, auth=HTTPBasicAuth(geoserver_user, geoserver_password))
        if response.status_code == 201:
            import_id = response.json()['import']['id']
            print(f"Import session created with ID: {import_id}")
            return import_id
        else:
            print(f"Failed to create import session. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error creating GeoTIFF import: {str(e)}")
        return None

# Function to upload the GeoTIFF file to the created import
def upload_geotiff_file(import_id, geoserver_url, geoserver_user, geoserver_password, tiff_file_path):
    upload_url = f"{geoserver_url}/rest/imports/{import_id}/tasks"
    
    try:
        # Step 2: Upload the GeoTIFF file
        with open(tiff_file_path, 'rb') as file:
            files = {
                'filedata': file
            }
            response = requests.post(upload_url, files=files, auth=HTTPBasicAuth(geoserver_user, geoserver_password))
            if response.status_code == 201:
                print(f"GeoTIFF file {tiff_file_path} uploaded successfully.")
            else:
                print(f"Failed to upload GeoTIFF. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error uploading GeoTIFF: {str(e)}")

# Function to execute the import
def execute_import(import_id, geoserver_url, geoserver_user, geoserver_password):
    execute_url = f"{geoserver_url}/rest/imports/{import_id}"
    
    try:
        # Step 3: Execute the import
        response = requests.post(execute_url, auth=HTTPBasicAuth(geoserver_user, geoserver_password))
        if response.status_code == 204:
            print(f"Import executed successfully.")
        else:
            print(f"Failed to execute import. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error executing import: {str(e)}")

# Example usage in an Airflow task
def upload_and_import_geotiff():
    workspace = "metashapergb"
    datastore_name = "	metashape_rgb"
    geoserver_url = "http://vps-52d8b534.vps.ovh.net:8084/geoserver/rest/workspaces/tests-geonetwork"
    geoserver_user = "admin"
    geoserver_password = "geoserver"
    tiff_file_path = "/path/to/your/geotiff/file.tif"
    
    # Step 1: Create import session
    import_id = create_geotiff_import(workspace, geoserver_url, geoserver_user, geoserver_password, tiff_file_path)
    
    if import_id:
        # Step 2: Upload the GeoTIFF file
        upload_geotiff_file(import_id, geoserver_url, geoserver_user, geoserver_password, tiff_file_path)
        
        # Step 3: Execute the import
        execute_import(import_id, geoserver_url, geoserver_user, geoserver_password)

# DAG and task definition
dag = DAG('metashape_rgb', description='Upload GeoTIFF to GeoServer using Importer API',
          schedule_interval='@once',
          start_date=datetime(2023, 1, 1), catchup=False)

upload_task = PythonOperator(
    task_id='upload_and_import_geotiff_task',
    python_callable=upload_and_import_geotiff,
    dag=dag
)
