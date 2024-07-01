import sys
from pyspark.sql import SparkSession

def show_text(spark, text, times):
    # Crea un RDD con el texto repetido la cantidad de veces especificada
    rdd = spark.sparkContext.parallelize([text] * times)
    
    # Recolecta los datos en el driver
    collected = rdd.collect()
    
    # Muestra el texto por consola desde el driver
    for item in collected:
        print(item)

if __name__ == "__main__":
    # Inicializa una sesión de Spark con configuraciones personalizadas
    spark = SparkSession.builder \
    .appName("DistributedShowTextApp") \
    # .master("spark://10.96.115.197:7077") \
    # .config("spark.executor.memory", "1g") \
    # .config("spark.executor.cores", "1") \
    # .config("spark.cores.max", "2") \
    .getOrCreate()
    
    try:
        # Llama a la función con el texto deseado y la cantidad de veces que debe mostrarse
        show_text(spark, "Hola, este es el texto que se muestra por consola.", 5)
    finally:
        # Detiene la sesión de Spark
        spark.stop()
