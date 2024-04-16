import pyspark

def spark_script_executor():
  """
  This function executes a simple Spark job to test the connection.
  """

  # Create a SparkSession
  spark = pyspark.sql.SparkSession.builder.appName("SparkTest").getOrCreate()

  data = spark.read.text("dato.txt")

  # Write the data to a new text file
  data.write.text("output_file.txt")  
  # Stop the SparkSession
  spark.stop()


if __name__ == "__main__":
  spark_script_executor()