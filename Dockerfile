# Dockerfile
FROM python:3.8-slim

WORKDIR /app

# Instalar dependencias necesarias
RUN pip install fpdf

# Copiar el script de conversión
COPY convert_json_to_pdf.py .

# Comando por defecto para ejecutar el script de conversión
CMD ["python", "convert_json_to_pdf.py"]
