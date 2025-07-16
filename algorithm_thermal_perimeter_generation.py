import json

from dag_utils import throw_job_error



def execute_thermal_perimeter_process(**context):
    """Ejecuta el algoritmo de perímetros térmicos utilizando los datos recibidos desde la interfaz."""

    conf = context.get("dag_run").conf
    if not conf:
        print("Error: No se recibió configuración desde el DAG.")
        return
    
    print("Datos recibidos del DAG:")
    print(json.dumps(conf, indent=4))

    message = conf.get("message", {})
    trace_id = conf.get("trace_id", "no-trace")
    print(f"Processing with trace_id: {trace_id}")
    
    job_id = message.get('id')
    input_data_str = message.get('input_data', '{}')
    
    try:
        input_data = json.loads(input_data_str)
    except json.JSONDecodeError:
        print("Error al decodificar 'input_data'")
        throw_job_error(job_id, "Invalid JSON in input_data")
        return

    # Extraer datos de la interfaz
    mission_id = input_data.get('mission_id')
    selected_bursts = input_data.get('selected_bursts', [])  # IDs de ráfagas seleccionadas
    
    if not mission_id:
        throw_job_error(job_id, "No se especificó ID de misión")
        return
    
    if not selected_bursts:
        throw_job_error(job_id, "No se seleccionaron ráfagas")
        return


    

