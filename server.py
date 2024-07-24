from flask import Flask, request, send_file
import os
from transform import json_to_pdf

app = Flask(__name__)

@app.route('/transform-json-to-pdf', methods=['POST'])
def transform_json_to_pdf():
    input_json = request.json
    output_pdf_path = '/data/file.pdf'
    
    # Transformar el JSON a PDF
    json_to_pdf(input_json, output_pdf_path)
    
    return send_file(output_pdf_path, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
