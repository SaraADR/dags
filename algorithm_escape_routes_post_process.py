from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json

def process_escape_routes_data(**context):
    # Obtener los datos del contexto del DAG
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str)

    # Procesar "inicio" y "destino" para permitir diferentes estructuras
    inicio = input_data.get('inicio', None)
    destino = input_data.get('destino', None)

    # Ajustar "inicio" y "destino" según los datos recibidos
    if isinstance(inicio, str):
        # Convertir string JSON en diccionario
        try:
            inicio = json.loads(inicio)
        except json.JSONDecodeError:
            inicio = None

    if isinstance(destino, list):
        # Convertir lista de strings JSON en lista de diccionarios
        try:
            destino = [json.loads(d) if isinstance(d, str) else d for d in destino]
        except json.JSONDecodeError:
            destino = None
    elif isinstance(destino, dict):
        # Usar directamente el destino si es un diccionario
        pass

    # Extraer los argumentos necesarios, incluyendo las características específicas
    params = {
        "dir_incendio": input_data.get('dir_incendio', None),
        "dir_mdt": input_data.get('dir_mdt', None),
        "dir_hojasmtn50": input_data.get('dir_hojasmtn50', None),
        "dir_combustible": input_data.get('dir_combustible', None),
        "api_idee": input_data.get('api_idee', True),  # Valor predeterminado True
        "dir_vias": input_data.get('dir_vias', None),
        "dir_cursos_agua": input_data.get('dir_cursos_agua', None),
        "dir_aguas_estancadas": input_data.get('dir_aguas_estancadas', None),
        "inicio": inicio,
        "destino": destino,
        "direccion_avance": input_data.get('direccion_avance', None),
        "distancia": input_data.get('distancia', None),
        "dist_seguridad": input_data.get('dist_seguridad', None),  # Propiedad adicional
        "dir_obstaculos": input_data.get('dir_obstaculos', None),
        "dir_carr_csv": input_data.get('dir_carr_csv', None),
        "dir_output": input_data.get('dir_output', None),
        "sugerir": input_data.get('sugerir', False),  # Valor predeterminado False
        "zonas_abiertas": input_data.get('zonas_abiertas', None),
        "v_viento": input_data.get('v_viento', None),
        "f_buffer": input_data.get('f_buffer', 100),  # Valor predeterminado 100
        "c_prop": input_data.get('c_prop', "Extremas"),  # Valor predeterminado "Extremas"
        "lim_pendiente": input_data.get('lim_pendiente', None),
        "dist_estudio": input_data.get('dist_estudio', 5000),  # Valor predeterminado 5000
    }

    # Crear el JSON dinámicamente
    json_data = create_json(params)

    # Mostrar el JSON por pantalla
    print("JSON generado:")
    print(json.dumps(json_data, indent=4))

def create_json(params):
    # Generar un JSON basado en los parámetros proporcionados
    input_data = {
        "dir_incendio": params.get("dir_incendio", None),
        "dir_mdt": params.get("dir_mdt", None),
        "dir_hojasmtn50": params.get("dir_hojasmtn50", None),
        "dir_combustible": params.get("dir_combustible", None),
        "api_idee": params.get("api_idee", True),
        "dir_vias": params.get("dir_vias", None),
        "dir_cursos_agua": params.get("dir_cursos_agua", None),
        "dir_aguas_estancadas": params.get("dir_aguas_estancadas", None),
        "inicio": params.get("inicio", None),
        "destino": params.get("destino", None),
        "direccion_avance": params.get("direccion_avance", None),
        "distancia": params.get("distancia", None),
        "dist_seguridad": params.get("dist_seguridad", None),
        "dir_obstaculos": params.get("dir_obstaculos", None),
        "dir_carr_csv": params.get("dir_carr_csv", None),
        "dir_output": params.get("dir_output", None),
        "sugerir": params.get("sugerir", False),
        "zonas_abiertas": params.get("zonas_abiertas", None),
        "v_viento": params.get("v_viento", None),
        "f_buffer": params.get("f_buffer", 100),
        "c_prop": params.get("c_prop", "Extremas"),
        "lim_pendiente": params.get("lim_pendiente", None),
        "dist_estudio": params.get("dist_estudio", 5000),
    }
    return input_data

# Configuración del DAG
default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'algorithm_escape_routes_post_process',
    default_args=default_args,
    description='DAG para generar JSON y mostrarlo por pantalla',
    schedule_interval=None,
    catchup=False,
    concurrency=1
)

# Tarea para generar y mostrar el JSON
process_escape_routes_task = PythonOperator(
    task_id='process_escape_routes',
    provide_context=True,
    python_callable=process_escape_routes_data,
    dag=dag,
)

# Cambiar el estado del job (si fuera necesario en un futuro)
change_state_task = PythonOperator(
    task_id='change_state_job',
    python_callable=lambda: print("Cambio de estado no implementado."),
    provide_context=True,
    dag=dag,
)

# Definir el flujo de tareas
process_escape_routes_task >> change_state_task
