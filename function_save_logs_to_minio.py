# function_save_logs_to_minio.py
import os
from datetime import datetime
from botocore.exceptions import ClientError
from dag_utils import get_minio_client

def save_logs_to_minio(**context):
    """
    Save logs from current execution to Minio
    """
    try:
        # Obtain the context variables
        dag_id = context['dag'].dag_id
        run_id = context['run_id']
        task_id = context['task'].task_id
        execution_date = context['execution_date']
        
        # Required date format: 20250325T093838
        formatted_date = execution_date.strftime('%Y%m%dT%H%M%S')
        
        # Logs base folder
        log_base_folder = "/opt/airflow/logs"
        print(f"Logs base folder: {log_base_folder}")
        
        # Build the path to the logs based on the observed structure
        # Example: /opt/airflow/logs/dag_id=dag_id/run_id=run_id
        # Note: The run_id is usually the execution date in Airflow
        # Example: /opt/airflow/logs/dag_id=example_dag/run_id=2023-10-01T00:00:00+00:00
        log_dir_path = f"{log_base_folder}/dag_id={dag_id}/run_id={run_id}"
        print(f"Searching logs directory in: {log_dir_path}")
        
        if not os.path.exists(log_dir_path):
            print(f"Logs directory doesn't exist: {log_dir_path}")
            # Try to list the parent directory to see if the DAG exists
            parent_dir = f"{log_base_folder}/dag_id={dag_id}"
            if os.path.exists(parent_dir):
                print(f"Content of the DAG's directory: {os.listdir(parent_dir)}")
            return
        
        # Serch for specific task logs
        task_logs = []
        for root, dirs, files in os.walk(log_dir_path):
            for file in files:
                if task_id in file or task_id in root:
                    full_path = os.path.join(root, file)
                    task_logs.append(full_path)
                    print(f"Log file found: {full_path}")
        
        if not task_logs:
            print(f"Logs for this task were not found: {task_id} en {log_dir_path}")
            # Show available files in the directory
            print("Available files:")
            for root, dirs, files in os.walk(log_dir_path):
                for file in files:
                    print(os.path.join(root, file))
            return
        
        # Use the first log file found
        log_path = task_logs[0]
        
        # Read the log file
        with open(log_path, 'r') as log_file:
            log_content = log_file.read()
        print(f"Log content successfully read, size: {len(log_content)} bytes")
        
        # Stablish connection to Minio
        s3_client = get_minio_client()
        
        # Save to Minio with the new format
        bucket_name = 'logs'
        key = f"airflow/{formatted_date}-{dag_id}.txt"
        
        # Check if the bucket exists, if not, create it
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except ClientError:
            print(f"Creando bucket: {bucket_name}")
            s3_client.create_bucket(Bucket=bucket_name)
        
        # Save the log content to Minio
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=log_content
        )
        print(f"Log saved successfully to Minio: bucket={bucket_name}, key={key}")
    except Exception as e:
        import traceback
        print(f"Error saving logs to Minio: {str(e)}")
        traceback.print_exc()