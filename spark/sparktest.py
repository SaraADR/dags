from pyspark.sql import SparkSession

def show_text(spark, text, times):
    # Crea un RDD con el texto repetido la cantidad de veces especificada
    rdd = spark.sparkContext.parallelize([text] * times)
    
    # Muestra el texto por consola
    rdd.foreach(lambda x: print(x))

if __name__ == "__main__":
    # Inicializa una sesión de Spark
    spark = SparkSession.builder.appName("ShowTextApp").getOrCreate()
    
    # Llama a la función con el texto deseado y la cantidad de veces que debe mostrarse
    show_text(spark, "Hola, este es el texto que se muestra por consola.", 5)
    
    # Detiene la sesión de Spark
    spark.stop()
