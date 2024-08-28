import json
import sys
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

def generate_pdf(input_json_path, output_pdf_path):
    # Leer los datos del archivo JSON
    with open(input_json_path, 'r') as json_file:
        data = json.load(json_file)

    # Crear un nuevo PDF
    c = canvas.Canvas(output_pdf_path, pagesize=letter)
    width, height = letter

    # Escribir datos en el PDF
    c.drawString(100, 750, "Datos del archivo JSON:")
    y_position = 700
    for key, value in data.items():
        c.drawString(100, y_position, f"{key}: {value}")
        y_position -= 20

    # Guardar el PDF
    c.save()

if __name__ == "__main__":
    input_json_path = sys.argv[1]
    output_pdf_path = sys.argv[2]
    generate_pdf(input_json_path, output_pdf_path)
