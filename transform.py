import pdfkit

def json_to_pdf(input_json, output_pdf):
    options = {
        'page-size': 'A4',
        'encoding': 'UTF-8',
    }

    # Crear el contenido HTML a partir del JSON
    html_content = f"<html><body><pre>{input_json}</pre></body></html>"

    # Convertir el HTML en un PDF
    pdfkit.from_string(html_content, output_pdf, options=options)
