import pyspark

def spark_script_executor():
  """
  This function executes a simple Spark job to test the connection.
  """

  # Create a SparkSession
  spark = pyspark.sql.SparkSession.builder.appName("SparkTest").getOrCreate()

  # Read data from a text file
  data = spark.read.text("dato.txt")

  # Count the number of lines
  line_count = data.count()

  # Print the line count
  print(f"Number of lines: {line_count}")

  # Stop the SparkSession
  spark.stop()

if __name__ == "__main__":
  spark_script_executor()