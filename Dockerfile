# Dockerfile

FROM python:3.9-slim

# Instalar dependencias
RUN apt-get update && \
    apt-get install -y wkhtmltopdf && \
    pip install pdfkit

# Copiar el script
COPY convert_to_pdf.py /app/convert_to_pdf.py

# Establecer el directorio de trabajo
WORKDIR /app

# Comando de entrada
ENTRYPOINT ["python", "convert_to_pdf.py"]
