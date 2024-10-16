import xml.etree.ElementTree as ET
import logging
import io
from datetime import datetime

# Configurar el logging
logging.basicConfig(level=logging.INFO)

# Función para crear el XML metadata
def creador_xml_metadata(file_identifier, organization_name, email_address, date_stamp, title, publication_date, west_bound, east_bound, south_bound, north_bound, spatial_resolution, protocol, wms_link, layer_name, layer_description):
    logging.info("Iniciando la creación del XML.")
    
    root = ET.Element("gmd:MD_Metadata", {
        "xmlns:gmd": "http://www.isotc211.org/2005/gmd",
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xmlns:gco": "http://www.isotc211.org/2005/gco",
        "xmlns:srv": "http://www.isotc211.org/2005/srv",
        "xmlns:gmx": "http://www.isotc211.org/2005/gmx",
        "xmlns:gts": "http://www.isotc211.org/2005/gts",
        "xmlns:gsr": "http://www.isotc211.org/2005/gsr",
        "xmlns:gmi": "http://www.isotc211.org/2005/gmi",
        "xmlns:gml": "http://www.opengis.net/gml/3.2",
        "xmlns:xlink": "http://www.w3.org/1999/xlink",
        "xsi:schemaLocation": "http://www.isotc211.org/2005/gmd http://schemas.opengis.net/csw/2.0.2/profiles/apiso/1.0.0/apiso.xsd"
    })

    # fileIdentifier
    fid = ET.SubElement(root, "gmd:fileIdentifier")
    fid_cs = ET.SubElement(fid, "gco:CharacterString")
    fid_cs.text = file_identifier

    # language
    language = ET.SubElement(root, "gmd:language")
    lang_code = ET.SubElement(language, "gmd:LanguageCode", {
        "codeList": "http://www.loc.gov/standards/iso639-2/",
        "codeListValue": "spa"
    })

    # characterSet
    char_set = ET.SubElement(root, "gmd:characterSet")
    char_set_code = ET.SubElement(char_set, "gmd:MD_CharacterSetCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_CharacterSetCode",
        "codeListValue": "utf8"
    })

    # parentIdentifier
    parent_identifier = ET.SubElement(root, "gmd:parentIdentifier", {"gco:nilReason": "missing"})

    # hierarchyLevel
    hierarchy = ET.SubElement(root, "gmd:hierarchyLevel")
    hierarchy_code = ET.SubElement(hierarchy, "gmd:MD_ScopeCode", {
        "codeList": "./resources/codelist.xml#MD_ScopeCode",
        "codeListValue": "dataset"
    })
    hierarchy_code.text = "dataset"

    # contact
    contact = ET.SubElement(root, "gmd:contact")
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

    # role
    role = ET.SubElement(responsible_party, "gmd:role")
    role_code = ET.SubElement(role, "gmd:CI_RoleCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_RoleCode",
        "codeListValue": "pointOfContact"
    })

    # dateStamp
    date_stamp_elem = ET.SubElement(root, "gmd:dateStamp")
    date_value = ET.SubElement(date_stamp_elem, "gco:DateTime")
    date_value.text = date_stamp

    # metadataStandardName
    metadata_standard = ET.SubElement(root, "gmd:metadataStandardName")
    metadata_standard_cs = ET.SubElement(metadata_standard, "gco:CharacterString")
    metadata_standard_cs.text = "NEM: ISO 19115:2003 + Reglamento (CE) Nº 1205/2008 de Inspire"

    # metadataStandardVersion
    metadata_version = ET.SubElement(root, "gmd:metadataStandardVersion")
    metadata_version_cs = ET.SubElement(metadata_version, "gco:CharacterString")
    metadata_version_cs.text = "1.2"

    # referenceSystemInfo
    ref_sys_info = ET.SubElement(root, "gmd:referenceSystemInfo")
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
    identification_info = ET.SubElement(root, "gmd:identificationInfo")
    md_data_identification = ET.SubElement(identification_info, "gmd:MD_DataIdentification")
    citation = ET.SubElement(md_data_identification, "gmd:citation")
    ci_citation = ET.SubElement(citation, "gmd:CI_Citation")
    title_elem = ET.SubElement(ci_citation, "gmd:title")
    title_cs = ET.SubElement(title_elem, "gco:CharacterString")
    title_cs.text = title

    # publication date
    pub_date = ET.SubElement(ci_citation, "gmd:date")
    ci_date = ET.SubElement(pub_date, "gmd:CI_Date")
    date = ET.SubElement(ci_date, "gmd:date")
    gco_date = ET.SubElement(date, "gco:Date")
    gco_date.text = publication_date
    date_type = ET.SubElement(ci_date, "gmd:dateType")
    ci_date_type = ET.SubElement(date_type, "gmd:CI_DateTypeCode", {
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_DateTypeCode",
        "codeListValue": "publication"
    })

    # bounding box
    extent = ET.SubElement(md_data_identification, "gmd:extent")
    ex_extent = ET.SubElement(extent, "gmd:EX_Extent")
    geographic_element = ET.SubElement(ex_extent, "gmd:geographicElement")
    bbox = ET.SubElement(geographic_element, "gmd:EX_GeographicBoundingBox")
    west_bound_elem = ET.SubElement(bbox, "gmd:westBoundLongitude")
    west_bound_cs = ET.SubElement(west_bound_elem, "gco:Decimal")
    west_bound_cs.text = west_bound
    east_bound_elem = ET.SubElement(bbox, "gmd:eastBoundLongitude")
    east_bound_cs = ET.SubElement(east_bound_elem, "gco:Decimal")
    east_bound_cs.text = east_bound
    south_bound_elem = ET.SubElement(bbox, "gmd:southBoundLatitude")
    south_bound_cs = ET.SubElement(south_bound_elem, "gco:Decimal")
    south_bound_cs.text = south_bound
    north_bound_elem = ET.SubElement(bbox, "gmd:northBoundLatitude")
    north_bound_cs = ET.SubElement(north_bound_elem, "gco:Decimal")
    north_bound_cs.text = north_bound

    # spatial resolution
    spatial_res = ET.SubElement(md_data_identification, "gmd:spatialResolution")
    distance = ET.SubElement(spatial_res, "gmd:MD_Resolution")
    dist_value = ET.SubElement(distance, "gmd:distance")
    dist_cs = ET.SubElement(dist_value, "gco:Distance", {"uom": "metros"})
    dist_cs.text = spatial_resolution

    # WMS linkage
    distribution_info = ET.SubElement(root, "gmd:distributionInfo")
    md_distribution = ET.SubElement(distribution_info, "gmd:MD_Distribution")
    transfer_options = ET.SubElement(md_distribution, "gmd:transferOptions")
    digital_transfer = ET.SubElement(transfer_options, "gmd:MD_DigitalTransferOptions")
    on_line = ET.SubElement(digital_transfer, "gmd:onLine")
    online_resource = ET.SubElement(on_line, "gmd:CI_OnlineResource")
    linkage = ET.SubElement(online_resource, "gmd:linkage")
    url = ET.SubElement(linkage, "gmd:URL")
    url.text = wms_link

    protocol_elem = ET.SubElement(online_resource, "gmd:protocol")
    protocol_cs = ET.SubElement(protocol_elem, "gco:CharacterString")
    protocol_cs.text = protocol

    name_elem = ET.SubElement(online_resource, "gmd:name")
    name_cs = ET.SubElement(name_elem, "gco:CharacterString")
    name_cs.text = layer_name

    desc_elem = ET.SubElement(online_resource, "gmd:description")
    desc_cs = ET.SubElement(desc_elem, "gco:CharacterString")
    desc_cs.text = layer_description

    logging.info("XML creado correctamente.")
    
    return ET.ElementTree(root)

# Función para generar el XML y guardarlo en un archivo
def generate_xml_test():
    logging.info("Iniciando la generación del XML para la prueba.")
    
    # JSON content as before
    json_content = {
        'fileIdentifier': 'Ortomosaico_testeo',
        'organizationName': 'Instituto geográfico nacional (IGN)',
        'email': 'ignis@organizacion.es',
        'dateStamp': datetime.now().isoformat(),
        'title': 'Ortomosaico_0026_4740004_611271',
        'publicationDate': '2024-07-29',
        'boundingBox': {
            'westBoundLongitude': '-7.6392',
            'eastBoundLongitude': '-7.6336',
            'southBoundLatitude': '42.8025',
            'northBoundLatitude': '42.8044'
        },
        'spatialResolution': '0.026',  # Resolución espacial en metros
        'protocol': 'OGC:WMS-1.3.0-http-get-map',
        'wmsLink': 'https://geoserver.dev.cuatrodigital.com/geoserver/tests-geonetwork/wms',
        'layerName': 'a__0026_4740004_611271',
        'layerDescription': 'Capa 0026 de prueba'
    }

    # Extract XML parameters (as before)
    file_identifier = json_content['fileIdentifier']
    organization_name = json_content['organizationName']
    email_address = json_content['email']
    date_stamp = json_content['dateStamp']
    title = json_content['title']
    publication_date = json_content['publicationDate']
    west_bound = json_content['boundingBox']['westBoundLongitude']
    east_bound = json_content['boundingBox']['eastBoundLongitude']
    south_bound = json_content['boundingBox']['southBoundLatitude']
    north_bound = json_content['boundingBox']['northBoundLatitude']
    spatial_resolution = json_content['spatialResolution']
    protocol = json_content['protocol']
    wms_link = json_content['wmsLink']
    layer_name = json_content['layerName']
    layer_description = json_content['layerDescription']

    # Generate XML tree
    tree = creador_xml_metadata(
        file_identifier=file_identifier,
        organization_name=organization_name,
        email_address=email_address,
        date_stamp=date_stamp,
        title=title,
        publication_date=publication_date,
        west_bound=west_bound,
        east_bound=east_bound,
        south_bound=south_bound,
        north_bound=north_bound,
        spatial_resolution=spatial_resolution,
        protocol=protocol,
        wms_link=wms_link,
        layer_name=layer_name,
        layer_description=layer_description
    )

    # Convert the XML tree to a string and print it
    xml_bytes_io = io.BytesIO()
    tree.write(xml_bytes_io, encoding='utf-8', xml_declaration=True)
    xml_content = xml_bytes_io.getvalue().decode('utf-8')

    # Save to a file or print to console
    with open("output_metadata.xml", "w", encoding="utf-8") as f:
        f.write(xml_content)

    logging.info(f"XML generado correctamente y guardado en 'output_metadata.xml'.")
    print("XML content:\n", xml_content)

# Llamar a la función de prueba
if __name__ == "__main__":
    generate_xml_test()
