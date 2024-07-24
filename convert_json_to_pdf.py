import json
from fpdf import FPDF

class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, 'JSON Data', 0, 1, 'C')

    def chapter_title(self, title):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, title, 0, 1, 'L')
        self.ln(10)

    def chapter_body(self, body):
        self.set_font('Arial', '', 12)
        self.multi_cell(0, 10, body)
        self.ln()

    def add_json(self, json_data):
        for key, value in json_data.items():
            self.chapter_title(key)
            if isinstance(value, dict):
                self.chapter_body(json.dumps(value, indent=4))
            else:
                self.chapter_body(str(value))

def convert_json_to_pdf(input_file, output_file):
    with open(input_file, 'r') as f:
        data = json.load(f)

    pdf = PDF()
    pdf.add_page()
    pdf.add_json(data)
    pdf.output(output_file)

if __name__ == "__main__":
    input_file = '/input/data.json'
    output_file = '/output/data.pdf'
    convert_json_to_pdf(input_file, output_file)
