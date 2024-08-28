# Usa una imagen base que tenga Python y las dependencias necesarias
FROM python:3.8-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia el archivo de requerimientos (si tienes dependencias de Python)
COPY requirements.txt .

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copia el script Python en el contenedor
COPY generate_pdf.py .

# Define el punto de entrada del contenedor
ENTRYPOINT ["python", "generate_pdf.py"]
