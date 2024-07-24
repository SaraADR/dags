# Usar una imagen base de Python
FROM python:3.9-slim

# Instalar dependencias necesarias
RUN pip install boto3

# Copiar el script Python al contenedor
COPY send_to_minio.py /app/send_to_minio.py

# Establecer el directorio de trabajo
WORKDIR /app

# Ejecutar el script
ENTRYPOINT ["python", "send_to_minio.py"]
