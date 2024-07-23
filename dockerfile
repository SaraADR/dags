# Use a base image with Python
FROM python:3.9-slim

# Install necessary Python packages
RUN pip install json2pdf

# Copy the script that converts JSON to PDF
COPY convert.py /app/convert.py

# Set the working directory
WORKDIR /app

# Command to run the script
ENTRYPOINT ["python", "convert.py"]
