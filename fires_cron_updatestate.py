# from datetime import datetime, timedelta, timezone
# from airflow import DAG
# from airflow.hooks.base_hook import BaseHook
# from sqlalchemy import create_engine, text
# from sqlalchemy.orm import sessionmaker
# from airflow.operators.python import PythonOperator

# def lookAtEinforexBd():
#     try:
#         # Establecer conexión a la base de datos
#         db_conn = BaseHook.get_connection('einforex_db')
#         connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/einforex"
#         engine = create_engine(connection_string)
#         Session = sessionmaker(bind=engine)
#         session = Session()

#         print("Conexión a la base de datos establecida correctamente")

#         # Definir la consulta SQL cruda
#         query = text("""
#             SELECT f.id, f.end, f.lastupdate, f.name
#             FROM einforex_fleet.fire f
#             WHERE f.lastupdate >= NOW() - INTERVAL '1 day'
#             ORDER BY f.id DESC;
#         """)

#         print("Ejecutando la consulta")
#         # Ejecutar la consulta
#         result = session.execute(query)


#         # Procesar y mostrar el resultado
#         rows = result.fetchall()  # Obtener todos los resultados
#         session.close()

#         db_conn = BaseHook.get_connection('biobd')
#         connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
#         engine = create_engine(connection_string)
#         Session = sessionmaker(bind=engine)
#         session = Session()

#         if rows:
#             for row in rows:
#                 print(row)
#                 fire_id = row.id  
#                 fire_end = row.end  
                
#                 if fire_end:  # Verificar si end tiene un valor
#                     # Actualizar la tabla mission en la otra base de datos
#                     print(f"fireend: {fire_end}")

#                     update_query = text("""
#                         UPDATE missions.mss_mission m
#                         SET end_date = :fire_end
#                         FROM missions.mss_mission_fire mf
#                         WHERE mf.fire_id = :fire_id AND mf.mission_id = m.id
#                     """)
                    
#                     # Ejecutar la actualización
#                     session.execute(update_query, {'fire_end': fire_end, 'fire_id': fire_id})
#                     print(f"Se ha actualizado la linea con fire_id: {fire_id}" )

#         else:
#             print("No se encontraron registros con lastupdate en las últimas 24 horas.")

#         # Confirmar cambios
#         session.commit()
#         session.close()
        
#     except Exception as e:
#         session.rollback()
#         print(f"Error durante la busqueda del customer_id: {str(e)}")





# default_args = {
#     'owner': 'sadr',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 8, 8),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=1),
# }

# dag = DAG(
#     'fires_cron_updatestate',
#     default_args=default_args,
#     description='Control de los incendios',
#     schedule_interval=timedelta(hours=1),
#     catchup=False
# )

# lookAtEinforexBd_task = PythonOperator(
#     task_id='lookAtEinforexBd_task',
#     python_callable=lookAtEinforexBd,
#     provide_context=True,
#     dag=dag,
# )

# lookAtEinforexBd_task