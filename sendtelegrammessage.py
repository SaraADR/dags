from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import requests
from airflow.utils.dates import days_ago

def send_telegram_message():
    # Obtén la conexión configurada en Airflow
    conn = BaseHook.get_connection('telegram_default')
    telegram_token = conn.password
    chat_id = "-4197964914"
    message = "Hello from Airflow!"

    # Configura la URL de la API de Telegram
    url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"

    # Envía el mensaje
    response = requests.post(url, data={
        'chat_id': chat_id,
        'text': message
    })

    # Verifica el resultado de la petición
    if response.status_code != 200:
        raise Exception(f"Error sending message: {response.text}")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    'send_telegram_message',
    default_args=default_args,
    description='A simple DAG to send a Telegram message',
    schedule_interval=None,
    catchup=False,
) as dag:

    send_message = PythonOperator(
        task_id='send_message',
        python_callable=send_telegram_message,
    )

    send_message
