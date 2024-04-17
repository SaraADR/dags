from pyspark.sql import SparkSession

# Configura la sesión de Spark
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# Lee el archivo de texto
text_file = spark.read.text("/opt/airflow/dags/repo/archivos/dato.txt")

# Divide cada línea en palabras
words = text_file.rdd.flatMap(lambda line: line.value.split())

# Cuenta el número de ocurrencias de cada palabra
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Muestra los resultados
for word, count in word_counts.collect():
    print(f"{word}: {count}")

# Detiene la sesión de Spark
spark.stop()
