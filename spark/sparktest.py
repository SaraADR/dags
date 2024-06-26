from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Example PySpark Job") \
    .getOrCreate()

# Tu código PySpark aquí
data = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
columns = ["Language", "Users"]

df = spark.createDataFrame(data).toDF(*columns)
df.show()

spark.stop()
