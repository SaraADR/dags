from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import filetype
from PIL import Image
from io import BytesIO
import base64

def process_message(content):
    kind = filetype.guess(content)

    if kind is not None and kind.mime.startswith('image/'):
        image = Image.open(BytesIO(content))
        image_metadata = image.info
        print("Image metadata:", image_metadata)
    else:
        print("Text message:", content.decode('utf-8'))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("KafkaMessageProcessor").getOrCreate()

    # Lee los mensajes de un archivo de texto (cada línea es un mensaje base64 codificado)
    messages_df = spark.read.text("/path/to/temp/messages.txt")
    
    # Procesa cada mensaje
    for row in messages_df.collect():
        message = base64.b64decode(row['value'])
        process_message(message)

    spark.stop()
