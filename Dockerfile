FROM python:3.9-slim

# Instalar dependencias necesarias
RUN apt-get update && apt-get install -y \
    wkhtmltopdf \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Instalar pdfkit y Flask
RUN pip install pdfkit Flask

# Copiar el script de transformación y el servidor Flask
COPY transform.py /app/transform.py
COPY server.py /app/server.py

WORKDIR /app

# Exponer el puerto para el servidor Flask
EXPOSE 5000

CMD ["python", "server.py"]
