from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.hooks.base_hook import BaseHook
import json
import pytz
from airflow.models import Variable
from dag_utils import execute_query
from sqlalchemy import text
from datetime import datetime, timedelta
import calendar

class FechaProxima:
    def __init__(self, fecha_str):
        # self.hoy = datetime.today()
        self.hoy = datetime.strptime(fecha_str, "%Y-%m-%d")

    def restar_meses(self, fecha, meses):
        mes = fecha.month - 1 - meses
        año = fecha.year + mes // 12
        mes = mes % 12 + 1
        dia = min(fecha.day, calendar.monthrange(año, mes)[1])
        return fecha.replace(year=año, month=mes, day=dia)
    
    def obtener_fechas_exactas(self, meses_minimo, meses_maximo):
        fechas = []
        for meses in range(int(meses_minimo), int(meses_maximo) + 1, int(meses_minimo)):
            fecha_resta = self.restar_meses(self.hoy, meses)
            if fecha_resta.day == self.hoy.day:
                fechas.append(fecha_resta.strftime("%Y-%m-%d"))
        return fechas

def process_element(**context):

    madrid_tz = pytz.timezone('Europe/Madrid')
    fechaHoraActual = datetime.now(madrid_tz)  # Fecha y hora con zona horaria

    print(f"Este algoritmo se está ejecutando a las {fechaHoraActual.strftime('%Y-%m-%d %H:%M:%S')} en Madrid, España")

    tipo2mesesminimo = Variable.get("dNBR_mesesFinIncendioMinimo", default_var="3")
    print(f"Valor de la variable tipo2mesesminimo en Airflow: {tipo2mesesminimo}")

    tipo2mesesmaximo = Variable.get("dNBR_mesesFinIncendioMaximo", default_var="1200")
    print(f"Valor de la variable tipo2mesesmaximo en Airflow: {tipo2mesesmaximo}")




    # Obtener fechas usando la clase FechaProxima
    fechas = FechaProxima()
    fechas_a_buscar = fechas.obtener_fechas(tipo2mesesminimo, tipo2mesesmaximo)
    print(f"Fechas calculadas: {fechas_a_buscar}")
    fechas_query = "','".join(fechas_a_buscar)

    query = f"""
        SELECT m.id, mf.fire_id, m.start_date, m.end_date
        FROM missions.mss_mission m
        JOIN missions.mss_mission_fire mf ON m.id = mf.mission_id
        WHERE mf.extinguishing_timestamp::DATE IN ('{fechas_query}')
    """

    result = execute_query('biobd', query)
    print(result)

default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'algorithm_dNBR_process_Type2',
    default_args=default_args,
    description='Algoritmo dNBR Type2',
    schedule_interval='*/2 * * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

process_element_task = PythonOperator(
    task_id='process_message',
    python_callable=process_element,
    provide_context=True,
    dag=dag,
)

process_element_task
