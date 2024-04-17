import pyspark
import requests
import sys

def aemetdownload( ):

  # Acceder a los argumentos pasados al script
  variable1 = sys.argv[1]
  variable2 = sys.argv[2]
  print(variable1)

  #INICIALIZACION DE VARIABLES
  #coordenadas = json_data['coordenadas']
  api_key = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzYXJhLmFycmliYXNAY3VhdHJvZGlnaXRhbC5jb20iLCJqdGkiOiIyOWMyZjJkMi1hNWM2LTQ4NmYtYWNhZC0xZTY1NjhiNWEwYzUiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTcxMjc0MzEyOSwidXNlcklkIjoiMjljMmYyZDItYTVjNi00ODZmLWFjYWQtMWU2NTY4YjVhMGM1Iiwicm9sZSI6IiJ9.Fev0ADUIPt-NBmMLDIEqrybWG9MUsKU12U_G2CyAo_4"
  #start = json_data['inicio_periodo']
  #finish = json_data['fin_periodo']
  error = ""
  derror = ""

  # Create a SparkSession
  spark = pyspark.sql.SparkSession.builder.appName("aemetdownload").getOrCreate()

  #data = spark.read.text("./municipios/municipios.shp")
  shapefile_df = spark.read.format("shape").load("./municipios/municipios.shp")
  shapefile_df.printSchema()
   


  ##-------------- TO DO: VER COMO GESTIONA ESTO CON LOS HUSOS ----------------------------------------------
  #if huso == 29:
  #    proj = 25829
  #else:
  #    proj = 25830 
  projlonlat = "epsg:4326"



  # #APARTADO DE MUNICIPIOS
  # # Lee los datos de los municipios desde un archivo
  # municipios = gpd.read_file('/opt/airflow/dags/repo/archivos/municipios/municipios.shp')   

  #   #municipios =  exec(open('/opt/airflow/dags/repo/archivos/municipios.csv').read())

  # # dirección para la predicción horaria por municipios de AEMET
  # url = "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/"
  # params = {
  # "api_key": api_key,
  # #"municipio": "CODIGO_MUNICIPIO"
  # }
  # urlpaste = url + '15061'
  # print(urlpaste)

  # try:
  #     response = requests.get(urlpaste, params=params)
  #     response.raise_for_status()  # Raise an exception for 4xx or 5xx errors
  #     data = response.json()  # Convert response to JSON
  #     print(data)
  #     print(data['datos'])
  # except requests.RequestException as e:
  #     # Manejar errores de solicitud aquí...
  #     print("Error al realizar la solicitud:", e)

  # try:
  #     response = requests.get(data['datos'], params=params)
  #     response.raise_for_status()  # Raise an exception for 4xx or 5xx errors
  #     data = response.json()  # Convert response to JSON
  #     print("Segunda ejecución")
  #     print(data)

  # except requests.RequestException as e:
  #     # Manejar errores de solicitud aquí...
  #     print("Error al realizar la solicitud:", e)

  spark.stop()
  return '12'

  # Stop the SparkSession



if __name__ == "__main__":
  aemetdownload()