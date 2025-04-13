from airflow.operators.python import ShortCircuitOperator, PythonOperator
from function_save_logs_to_minio import save_logs_to_minio

def setup_conditional_log_saving(dag, 
                                  task_id='save_logs_to_minio',
                                  task_id_to_save=None,
                                  condition_function=None):
    """
    Set tasks to save logs only under condition.

    Args:
        dag: DAG in which the tasks are registered.
        task_id: task name of saving logs task.
        condition_function: it returns True if logs have to be saved.

    Returns:
        tuple: (check_task, save_task)
    """

    if not callable(condition_function):
        raise ValueError("A valid 'condition_function' is required.")
    if not task_id_to_save:
        raise ValueError("You must specify 'task_id_to_save'.")

    check_activity = ShortCircuitOperator(
        task_id='check_activity_for_logs',
        python_callable=condition_function,
        provide_context=True,
        dag=dag,
    )

    save_logs_task = PythonOperator(
        task_id=task_id,
        python_callable=save_logs_to_minio,
        provide_context=True,
        op_kwargs={'task_id_to_save': task_id_to_save},
        dag=dag,
    )

    check_activity >> save_logs_task

    return check_activity, save_logs_task
