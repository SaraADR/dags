# convert_to_pdf.py

import pdfkit
import sys

def convert_to_pdf(input_file, output_file):
    # Asegúrate de tener wkhtmltopdf instalado en tu contenedor
    pdfkit.from_file(input_file, output_file)

if __name__ == "__main__":
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    convert_to_pdf(input_file, output_file)
