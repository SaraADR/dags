import sys
import json
from fpdf import FPDF

def json_to_pdf(json_data, pdf_path):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    
    for key, value in json_data.items():
        if isinstance(value, dict):
            pdf.cell(200, 10, txt=f"{key}:", ln=True)
            for sub_key, sub_value in value.items():
                pdf.cell(200, 10, txt=f"  {sub_key}: {sub_value}", ln=True)
        elif isinstance(value, list):
            pdf.cell(200, 10, txt=f"{key}:", ln=True)
            for item in value:
                pdf.cell(200, 10, txt=f"  - {item}", ln=True)
        else:
            pdf.cell(200, 10, txt=f"{key}: {value}", ln=True)
    
    pdf.output(pdf_path)

if __name__ == "__main__":
    json_path = "/app/input.json"
    pdf_path = "/app/output.pdf"
    
    with open(json_path, "r") as json_file:
        json_data = json.load(json_file)
    
    json_to_pdf(json_data, pdf_path)
