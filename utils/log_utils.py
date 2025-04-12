from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator  # ← Asegúrate de importar esto

def setup_conditional_log_saving(dag, task_id='save_logs_to_minio', condition_function=None):
    """
    Set conditional logs saving.
    """
    def _branch(**context):
        if condition_function(**context):
            return task_id  # Ejecutar save_logs_to_minio
        return "skip_save_logs"  # Saltar (no hacer nada)

    check_activity = BranchPythonOperator(
        task_id='check_activity_for_logs',
        python_callable=_branch,
        provide_context=True,
        dag=dag,
    )

    save_logs_task = PythonOperator(
        task_id=task_id,
        python_callable=save_logs_to_minio,
        provide_context=True,
        dag=dag,
    )

    skip_task = EmptyOperator(  # ← Task "dummy" para la rama de salto
        task_id="skip_save_logs",
        dag=dag,
    )

    check_activity >> [save_logs_task, skip_task]  # ← Conexión explícita de ramas
    return check_activity, save_logs_task