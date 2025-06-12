import base64
from airflow.hooks.base_hook import BaseHook
import requests
import logging
import os
import io

def get_geonetwork_credentials():
    try:

        conn = BaseHook.get_connection('geonetwork_credentials')
        credential_dody = {
            "username" : conn.login,
            "password" : conn.password
        }

        # Hacer la solicitud para obtener las credenciales
        logging.info(f"Obteniendo credenciales de: {conn.host}")
        response = requests.post(conn.host,json= credential_dody)

        # Verificar que la respuesta sea exitosa
        response.raise_for_status()

        # Extraer los headers y tokens necesarios
        response_object = response.json()
        access_token = response_object['accessToken']
        xsrf_token = response_object['xsrfToken']
        set_cookie_header = response_object['setCookieHeader']
    

        return [access_token, xsrf_token, set_cookie_header]
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al obtener credenciales: {e}")
        raise Exception(f"Error al obtener credenciales: {e}")

def agregar_pdf_y_re_subir_simple(xml_base64, uuid_var, nombre):
    # Datos de conexión
    connection = BaseHook.get_connection("geonetwork_connection")
    base_url = f"{connection.schema}{connection.host}"
    access_token, xsrf_token, set_cookie_header = get_geonetwork_credentials()
    # Decodificamos XML
    xml_str = base64.b64decode(xml_base64).decode('utf-8')

    # Bloque XML del onLine
    online_block = f"""
    <gmd:onLine xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gco="http://www.isotc211.org/2005/gco">
      <gmd:CI_OnlineResource>
        <gmd:linkage>
          <gmd:URL>{base_url}/geonetwork/srv/api/records/{uuid_var}/attachments/{nombre}</gmd:URL>
        </gmd:linkage>
        <gmd:name>
          <gco:CharacterString>{nombre}</gco:CharacterString>
        </gmd:name>
        <gmd:description>
          <gco:CharacterString>Informe técnico de la misión</gco:CharacterString>
        </gmd:description>
        <gmd:function>
          <gmd:CI_OnLineFunctionCode codeListValue="download" codeList="https://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"/>
        </gmd:function>
      </gmd:CI_OnlineResource>
    </gmd:onLine>
    """

    # Insertamos justo antes del cierre de distributionInfo
    if "</gmd:distributionInfo>" in xml_str:
        xml_str_mod = xml_str.replace("</gmd:distributionInfo>", f"{online_block}\n</gmd:distributionInfo>")
    else:
        raise Exception("No se encontró <gmd:distributionInfo> en el XML original")

    # Codificamos nuevamente
    xml_mod = xml_str_mod.encode('utf-8')

    # Subida a GeoNetwork
    upload_url = f"{base_url}/geonetwork/srv/api/records"
    headers = {
        'Authorization': f"Bearer {access_token}",
        'x-xsrf-token': str(xsrf_token),
        'Cookie': str(set_cookie_header[0]),
        'Accept': 'application/json'
    }

    files = {
        'file': ('metadata.xml', xml_mod, 'text/xml'),
    }

    params = {
        "uuidProcessing": "OVERWRITE"
    }

    response = requests.post(upload_url, files=files, headers=headers, params=params)
    print(f"GeoNetwork response: {response.status_code} - {response.text}")
    response.raise_for_status()


def upload_to_geonetwork_xml(xml_data_array):
        try:
            connection = BaseHook.get_connection("geonetwork_connection")
            upload_url = f"{connection.schema}{connection.host}/geonetwork/srv/api/records"
            access_token, xsrf_token, set_cookie_header = get_geonetwork_credentials()

            resource_ids = []

            for xml_data in xml_data_array:
                xml_decoded = base64.b64decode(xml_data).decode('utf-8')

                files = {
                    'file': ('metadata.xml', xml_decoded, 'text/xml'),
                }

                headers = {
                    'Authorization': f"Bearer {access_token}",
                    'x-xsrf-token': str(xsrf_token),
                    'Cookie': str(set_cookie_header[0]),
                    'Accept': 'application/json'
                }
                params = {
                    "uuidProcessing": "OVERWRITE"
                }

                response = requests.post(upload_url, files=files, headers=headers , params=params)
                logging.info(f"Respuesta completa de GeoNetwork: {response.status_code}, {response.text}")

                response.raise_for_status()
                response_data = response.json()

                metadata_infos = response_data.get("metadataInfos", {})
                if metadata_infos:
                    metadata_values = list(metadata_infos.values())[0]
                    if metadata_values:
                        resource_id = metadata_values[0].get("uuid")
                    else:
                        resource_id = None
                else:
                    resource_id = None

                if not resource_id:
                    logging.error(f"No se encontró un identificador válido en la respuesta: {response_data}")
                    continue

                logging.info(f"UUID del recurso: {resource_id}")
                resource_ids.append(resource_id)

            if not resource_ids:
                raise Exception("No se generó ningún resource_id.")

            return resource_ids

        except Exception as e:
            logging.error(f"Error al subir el archivo a GeoNetwork: {e}")
            raise


def upload_tiff_attachment(resource_ids, metadata_input, archivos):
        connection = BaseHook.get_connection("geonetwork_connection")
        base_url = f"{connection.schema}{connection.host}/geonetwork/srv/api"
        access_token, xsrf_token, set_cookie_header = get_geonetwork_credentials()
        geonetwork_url = connection.host 

        for archivo in archivos:
            archivo_file_name = os.path.basename(archivo['file_name'])
            archivo_content = base64.b64decode(archivo['content'])
            print("--------------------  XXX  ------------------------")
            print(archivo_file_name)


            ext = os.path.splitext(archivo_file_name)[1].lower()
            mime_type = {
                    '.tif': 'image/tiff',
                    '.tiff': 'image/tiff',
                    '.png': 'image/png',
                    '.pdf': 'application/pdf',
                    '.txt': 'text/plain',
                    '.sh': 'application/x-sh'  
            }.get(ext, 'application/octet-stream')
            
            for uuid in resource_ids:
                files = {
                    'file': (archivo_file_name, io.BytesIO(archivo_content), mime_type),
                }

                headers = {
                    'Authorization': f"Bearer {access_token}",
                    'x-xsrf-token': str(xsrf_token),
                    'Cookie': str(set_cookie_header[0]),
                    'Accept': 'application/json'
                }

                url = f"{geonetwork_url}/geonetwork/srv/api/records/{uuid}/attachments"
                response = requests.post(url, files=files, headers=headers)

                if response.status_code not in [200, 201]:
                    logging.error(f"Error subiendo recurso a {uuid}: {response.status_code} {response.text}")
                    raise Exception("Fallo al subir el adjunto")
                else:
                    logging.info(f"Recurso subido correctamente para {uuid}")