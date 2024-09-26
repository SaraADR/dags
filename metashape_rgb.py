from collections import defaultdict
from datetime import datetime, timedelta
import os
import re
import tempfile
from airflow import DAG
import requests
from requests.auth import HTTPBasicAuth
from airflow.operators.python import PythonOperator
import xml.etree.ElementTree as ET


# Primer paso: leer y procesar los archivos
def process_extracted_files(**kwargs):
    # Obtenemos los archivos
    otros = kwargs['dag_run'].conf.get('otros', [])
    json_content = kwargs['dag_run'].conf.get('json')

    if not json_content:
        print("Ha habido un error con el traspaso de los documentos")
        return

    # Parametrizar los datos del metadata
    file_identifier = json_content.get('fileIdentifier', 'Ortomosaico_0026_4740004_611271')
    organization_name = json_content.get('organizationName', 'Instituto geográfico nacional (IGN)')
    email_address = json_content.get('email', 'ignis@organizacion.es')
    date_stamp = json_content.get('dateStamp', datetime.now().isoformat())
    title = json_content.get('title', 'Ortomosaico_0026_4740004_611271')

    with tempfile.TemporaryDirectory() as temp_dir:

        xml_file_path = os.path.join(temp_dir, "archivo.xml")
           
        # Crear el contenido del XML
        tree = creador_xml_metadata (
            file_identifier=file_identifier,
            organization_name=organization_name,
            email_address=email_address,
            date_stamp=date_stamp,
            title=title,

        )

        # Guardar el archivo XML
        tree.write(xml_file_path, encoding='utf-8', xml_declaration=True)
        
        # Imprimir la ruta del archivo XML para verificar
        print(f"Archivo XML guardado en: {xml_file_path}")
        
        # Leer el archivo XML para verificar su contenido
        with open(xml_file_path, 'r') as file:
            print(file.read())

# Al finalizar el bloque with, el archivo y el directorio temporal se eliminan automáticamente
    
def creador_xml_metadata(file_identifier, organization_name, email_address, date_stamp, title):
    root = ET.Element("root")
    md = ET.SubElement(root, "gmd:MD_Metadata")
    # Atributos XML Namespace
    md.attrib["xmlns:gmd"] = "http://www.isotc211.org/2005/gmd"
    md.attrib["xmlns:xsi"] = "http://www.w3.org/2001/XMLSchema-instance"
    md.attrib["xmlns:gco"] = "http://www.isotc211.org/2005/gco"
    md.attrib["xmlns:srv"] = "http://www.isotc211.org/2005/srv"
    md.attrib["xmlns:gmx"] = "http://www.isotc211.org/2005/gmx"
    md.attrib["xmlns:gts"] = "http://www.isotc211.org/2005/gts"
    md.attrib["xmlns:gsr"] = "http://www.isotc211.org/2005/gsr"
    md.attrib["xmlns:gmi"] = "http://www.isotc211.org/2005/gmi"
    md.attrib["xmlns:gml"] = "http://www.opengis.net/gml/3.2"
    md.attrib["xmlns:xlink"] = "http://www.w3.org/1999/xlink"
    md.attrib["xsi:schemaLocation"] = "http://www.isotc211.org/2005/gmd http://schemas.opengis.net/csw/2.0.2/profiles/apiso/1.0.0/apiso.xsd"

    # fileIdentifier
    fid = ET.SubElement(md, "gmd:fileIdentifier")
    fid_cs = ET.SubElement(fid, "gco:CharacterString")
    fid_cs.text = file_identifier

    # language
    language = ET.SubElement(md, "gmd:language")
    lang_code = ET.SubElement(language, "gmd:LanguageCode")
    lang_code.attrib["codeList"] = "http://www.loc.gov/standards/iso639-2/"
    lang_code.attrib["codeListValue"] = "spa"

    # characterSet
    char_set = ET.SubElement(md, "gmd:characterSet")
    char_set_code = ET.SubElement(char_set, "gmd:MD_CharacterSetCode")
    char_set_code.attrib["codeList"] = "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_CharacterSetCode"
    char_set_code.attrib["codeListValue"] = "utf8"

    # hierarchyLevel
    hierarchy = ET.SubElement(md, "gmd:hierarchyLevel")
    hierarchy_code = ET.SubElement(hierarchy, "gmd:MD_ScopeCode")
    hierarchy_code.attrib["codeList"] = "./resources/codelist.xml#MD_ScopeCode"
    hierarchy_code.attrib["codeListValue"] = "dataset"
    hierarchy_code.text = "dataset"

    # contact information
    contact = ET.SubElement(md, "gmd:contact")
    responsible_party = ET.SubElement(contact, "gmd:CI_ResponsibleParty")
    org_name = ET.SubElement(responsible_party, "gmd:organisationName")
    org_name_cs = ET.SubElement(org_name, "gco:CharacterString")
    org_name_cs.text = organization_name

    # contact email
    contact_info = ET.SubElement(responsible_party, "gmd:contactInfo")
    ci_contact = ET.SubElement(contact_info, "gmd:CI_Contact")
    address = ET.SubElement(ci_contact, "gmd:address")
    ci_address = ET.SubElement(address, "gmd:CI_Address")
    email = ET.SubElement(ci_address, "gmd:electronicMailAddress")
    email_cs = ET.SubElement(email, "gco:CharacterString")
    email_cs.text = email_address

    # dateStamp
    date_stamp_elem = ET.SubElement(md, "gmd:dateStamp")
    date_value = ET.SubElement(date_stamp_elem, "gco:DateTime")
    date_value.text = date_stamp

    # metadataStandardName
    metadata_standard = ET.SubElement(md, "gmd:metadataStandardName")
    metadata_standard_cs = ET.SubElement(metadata_standard, "gco:CharacterString")
    metadata_standard_cs.text = "NEM: ISO 19115:2003 + Reglamento (CE) Nº 1205/2008 de Inspire"

    # metadataStandardVersion
    metadata_version = ET.SubElement(md, "gmd:metadataStandardVersion")
    metadata_version_cs = ET.SubElement(metadata_version, "gco:CharacterString")
    metadata_version_cs.text = "1.2"

    # referenceSystemInfo
    ref_sys_info = ET.SubElement(md, "gmd:referenceSystemInfo")
    md_ref_sys = ET.SubElement(ref_sys_info, "gmd:MD_ReferenceSystem")
    ref_sys_id = ET.SubElement(md_ref_sys, "gmd:referenceSystemIdentifier")
    rs_id = ET.SubElement(ref_sys_id, "gmd:RS_Identifier")
    code = ET.SubElement(rs_id, "gmd:code")
    code_cs = ET.SubElement(code, "gco:CharacterString")
    code_cs.text = "EPSG:32629"
    code_space = ET.SubElement(rs_id, "gmd:codeSpace")
    code_space_cs = ET.SubElement(code_space, "gco:CharacterString")
    code_space_cs.text = "http://www.ign.es"

    # identificationInfo
    identification_info = ET.SubElement(md, "gmd:identificationInfo")
    md_data_identification = ET.SubElement(identification_info, "gmd:MD_DataIdentification")
    citation = ET.SubElement(md_data_identification, "gmd:citation")
    ci_citation = ET.SubElement(citation, "gmd:CI_Citation")
    title_elem = ET.SubElement(ci_citation, "gmd:title")
    title_cs = ET.SubElement(title_elem, "gco:CharacterString")
    title_cs.text = title

    # Convertir el árbol en un string XML
    tree = ET.ElementTree(root)
    return tree




# Configuración del DAG de Airflow
default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'metashape_rgb',
    default_args=default_args,
    description='Flujo de datos de entrada de elementos de metashape_rgb',
    schedule_interval=None,
    catchup=False,
)

# Primera tarea: procesar los archivos
process_extracted_files_task = PythonOperator(
    task_id='process_extracted_files_task',
    python_callable=process_extracted_files,
    provide_context=True,
    dag=dag,
)

# # Segunda tarea: subir los archivos a GeoServer
# upload_files_to_geoserver_task = PythonOperator(
#     task_id='upload_files_to_geoserver_task',
#     python_callable=upload_files_to_geoserver,
#     provide_context=True,
#     dag=dag,
# )

# Definir la secuencia de las tareas
process_extracted_files_task 
# >> upload_files_to_geoserver_task
