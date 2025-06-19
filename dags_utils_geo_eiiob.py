import base64
import datetime
import logging
import os
from dag_utils import get_geoserver_connection
import requests
import io
import rasterio
from xml.sax.saxutils import escape
from airflow.hooks.base import BaseHook

def obtener_coordenadas_tif(name, content):
    with rasterio.open(io.BytesIO(content)) as dataset:
        bounds = dataset.bounds  
        coordenadas = {
            "min_longitud": bounds.left,
            "max_longitud": bounds.right,
            "min_latitud": bounds.bottom,
            "max_latitud": bounds.top
        }
    
    return coordenadas


def publish_to_geoserver(archivos, WORKSPACE, GENERIC_LAYER, **context):

    if not archivos:
        raise Exception("No hay archivos para subir a GeoServer.")
    
    # Subida a GeoServer
    base_url, auth = get_geoserver_connection("geoserver_connection")
    temp_files = []
    
    for archivo in archivos:
        archivo_file_name = archivo['file_name']
        archivo_content = base64.b64decode(archivo['content'])

        archivo_file_name = os.path.basename(archivo_file_name)
        archivo_extension = os.path.splitext(archivo_file_name)[1]

        nombre_base = os.path.splitext(archivo_file_name)[0]
        temp_file_path = os.path.join("/tmp", f"{nombre_base}{archivo_extension}")

        # Guardar el archivo en el sistema antes de usarlo
        with open(temp_file_path, 'wb') as temp_file:
            temp_file.write(archivo_content)

        temp_files.append((archivo_file_name, temp_file_path))
   

    #Seleccionamos los tiff
    tiff_files = [path for name, path in temp_files if name.lower().endswith(('.tif', '.tiff'))]
    wms_layers_info = []
    wms_server_tiff = None
    wms_layer_tiff = None
    wms_description_tiff = "Capa publicada en GeoServer"

    
    for tif_file in tiff_files:
        with open(tif_file, 'rb') as f:
            file_data = f.read()
        
        layer_name = f"{GENERIC_LAYER}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        headers = {"Content-type": "image/tiff"}

        # Publicar capa raster en GeoServer
        url_new = f"{base_url}/workspaces/{WORKSPACE}/coveragestores/{layer_name}/file.geotiff"
        response = requests.put(url_new, headers=headers, data=file_data, auth=auth, params={"configure": "all"})
        if response.status_code not in [201, 202]:
            raise Exception(f"Error publicando {layer_name}: {response.text}")
        print(f"Capa raster publicada: {layer_name}")

        wms_server_tiff = f"{base_url}/{WORKSPACE}/wms"
        wms_layer_tiff = f"{WORKSPACE}:{layer_name}"
        wms_layers_info.append({
            "wms_server": wms_server_tiff,
            "wms_layer": wms_layer_tiff,
            "layer_name": layer_name,
            "wms_description_tiff" : wms_description_tiff
        })
        try:
            # Actualizar capa genérica
            url_latest = f"{base_url}/workspaces/{WORKSPACE}/coveragestores/{GENERIC_LAYER}/file.geotiff"
            response_latest = requests.put(url_latest, headers=headers, data=file_data, auth=auth, params={"configure": "all"})
            if response_latest.status_code not in [201, 202]:
                raise Exception(f"Error actualizando capa genérica: {response_latest.text}")
            print(f"Capa genérica raster actualizada: {GENERIC_LAYER}")
            print(f"Raster disponible en: {base_url}/geoserver/{WORKSPACE}/wms?layers={WORKSPACE}:{layer_name}")
        except Exception as e:
            print(f"Capa generica ya levantada: {e}")

    return wms_layers_info


def generar_graphic_overview(images_files):
    bloques = []
    for image in images_files:
        ruta_png = image["content"]
        bloque = f"""
        <gmd:graphicOverview>
            <gmd:MD_BrowseGraphic>
                <gmd:fileName>
                    <gco:CharacterString>{ruta_png}</gco:CharacterString>
                </gmd:fileName>
            </gmd:MD_BrowseGraphic>
        </gmd:graphicOverview>
        """
        bloques.append(bloque)
    return "\n".join(bloques)


def generate_dynamic_xml(eiiob_titulo, eiiob_descripcion, eiiob_inspire, eiiob_categoria,eiiob_pkey,eiiob_idioma,eiiob_representacion, eiiob_referencia, bbox, wms_layers_info, images_files):



    if bbox:
        min_longitud = bbox["min_longitud"]
        max_longitud = bbox["max_longitud"]
        min_latitud = bbox["min_latitud"]
        max_latitud = bbox["max_latitud"]
    else:
        # Puedes definir valores por defecto o lanzar un warning si es necesario
        print("No se encontró el archivo fire.dNBR.tif para extraer el bbox.")
        min_longitud = max_longitud = min_latitud = max_latitud = None

    publication_date = datetime.now().strftime("%Y-%m-%d")
    graphic_overview_xml = generar_graphic_overview(images_files)
    gmd_online_resources = generar_gmd_online(wms_layers_info)

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
    <gmd:MD_Metadata 
        xmlns:gmd="http://www.isotc211.org/2005/gmd"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns:gco="http://www.isotc211.org/2005/gco"
        xmlns:srv="http://www.isotc211.org/2005/srv"
        xmlns:gmx="http://www.isotc211.org/2005/gmx"
        xmlns:gts="http://www.isotc211.org/2005/gts"
        xmlns:gsr="http://www.isotc211.org/2005/gsr"
        xmlns:gmi="http://www.isotc211.org/2005/gmi"
        xmlns:gml="http://www.opengis.net/gml/3.2"
        xmlns:xlink="http://www.w3.org/1999/xlink"
        xsi:schemaLocation="http://www.isotc211.org/2005/gmd 
                            http://schemas.opengis.net/csw/2.0.2/profiles/apiso/1.0.0/apiso.xsd">

                            
        <gmd:language>
            <gmd:LanguageCode codeList="http://www.loc.gov/standards/iso639-2/" codeListValue="es"/>
        </gmd:language>

        <gmd:characterSet>
            <gmd:MD_CharacterSetCode codeListValue="utf8"
                                codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_CharacterSetCode"/>
        </gmd:characterSet>

        <gmd:hierarchyLevel>
            <gmd:MD_ScopeCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_ScopeCode"
                                codeListValue="dataset"/>
        </gmd:hierarchyLevel>


        <gmd:contact>
            <gmd:CI_ResponsibleParty>
                <gmd:individualName>
                    <gco:CharacterString>I+D</gco:CharacterString>
                </gmd:individualName>
                <gmd:organisationName>
                    <gco:CharacterString>Avincis</gco:CharacterString>
                </gmd:organisationName>
                <gmd:contactInfo>
                    <gmd:CI_Contact>
                    <gmd:address>
                        <gmd:CI_Address>
                            <gmd:electronicMailAddress>
                                <gco:CharacterString>soporte@einforex.es</gco:CharacterString>
                            </gmd:electronicMailAddress>
                        </gmd:CI_Address>
                    </gmd:address>
                    <gmd:onlineResource>
                        <gmd:CI_OnlineResource>
                            <gmd:linkage>
                                <gmd:URL>https://www.avincis.com</gmd:URL>
                            </gmd:linkage>
                            <gmd:protocol gco:nilReason="missing">
                                <gco:CharacterString/>
                            </gmd:protocol>
                            <gmd:name gco:nilReason="missing">
                                <gco:CharacterString/>
                            </gmd:name>
                            <gmd:description gco:nilReason="missing">
                                <gco:CharacterString/>
                            </gmd:description>
                        </gmd:CI_OnlineResource>
                    </gmd:onlineResource>
                    </gmd:CI_Contact>
                </gmd:contactInfo>
                <gmd:role>
                    <gmd:CI_RoleCode codeListValue="author"
                                    codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_RoleCode"/>
                </gmd:role>
            </gmd:CI_ResponsibleParty>
        </gmd:contact>

        
        <gmd:dateStamp>
            <gco:DateTime>${publication_date}</gco:DateTime>
        </gmd:dateStamp>

        <gmd:metadataStandardName>
            <gco:CharacterString>ISO 19115:2003/19139</gco:CharacterString>
        </gmd:metadataStandardName>


        <gmd:metadataStandardVersion>
            <gco:CharacterString>1.0</gco:CharacterString>
        </gmd:metadataStandardVersion>


        <gmd:identificationInfo>
            <srv:SV_ServiceIdentification>
                <gmd:citation>
                    <gmd:CI_Citation>
                    <gmd:title>
                        <gco:CharacterString>{eiiob_titulo}</gco:CharacterString>
                    </gmd:title>
                    <gmd:date>
                        <gmd:CI_Date>
                            <gmd:date>
                                <gco:Date>${publication_date}</gco:Date>
                            </gmd:date>
                            <gmd:dateType>
                                <gmd:CI_DateTypeCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_DateTypeCode"
                                                    codeListValue="publication"/>
                            </gmd:dateType>
                        </gmd:CI_Date>
                    </gmd:date>
                    </gmd:CI_Citation>
                </gmd:citation>

                {graphic_overview_xml}

                <gmd:abstract>
                    <gco:CharacterString>{eiiob_descripcion}</gco:CharacterString>
                </gmd:abstract>
                <gmd:status>
                    <gmd:MD_ProgressCode codeListValue="completed"
                                        codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_ProgressCode"/>
                </gmd:status>
                <gmd:pointOfContact>
                    <gmd:CI_ResponsibleParty>
                    <gmd:individualName>
                        <gco:CharacterString>I+D</gco:CharacterString>
                    </gmd:individualName>
                    <gmd:organisationName>
                        <gco:CharacterString>Avincis</gco:CharacterString>
                    </gmd:organisationName>
                    <gmd:contactInfo>
                        <gmd:CI_Contact>
                            <gmd:address>
                                <gmd:CI_Address>
                                <gmd:electronicMailAddress>
                                    <gco:CharacterString>soporte@einforex.es</gco:CharacterString>
                                </gmd:electronicMailAddress>
                                </gmd:CI_Address>
                            </gmd:address>
                            <gmd:onlineResource>
                                <gmd:CI_OnlineResource>
                                <gmd:linkage>
                                    <gmd:URL>https://www.avincis.com</gmd:URL>
                                </gmd:linkage>
                                <gmd:protocol gco:nilReason="missing">
                                    <gco:CharacterString/>
                                </gmd:protocol>
                                <gmd:name gco:nilReason="missing">
                                    <gco:CharacterString/>
                                </gmd:name>
                                <gmd:description gco:nilReason="missing">
                                    <gco:CharacterString/>
                                </gmd:description>
                                </gmd:CI_OnlineResource>
                            </gmd:onlineResource>
                        </gmd:CI_Contact>
                    </gmd:contactInfo>
                    <gmd:role>
                        <gmd:CI_RoleCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_RoleCode"
                                        codeListValue="author"/>
                    </gmd:role>
                    </gmd:CI_ResponsibleParty>
                </gmd:pointOfContact>


                <gmd:dateStamp>
                    <gco:Date>{publication_date}</gco:Date>
                </gmd:dateStamp>

                <gmd:resourceMaintenance>
                    <gmd:MD_MaintenanceInformation>
                        <gmd:maintenanceAndUpdateFrequency>
                            <gmd:MD_MaintenanceFrequencyCode codeList="http://www.isotc211.org/2005/resources/codeList.xml#MD_MaintenanceFrequencyCode"
                                                            codeListValue="continual">Continualmente</gmd:MD_MaintenanceFrequencyCode>
                        </gmd:maintenanceAndUpdateFrequency>
                        <gmd:maintenanceAndUpdateFrequency>
                            <gmd:MD_MaintenanceFrequencyCode codeList="http://www.isotc211.org/2005/resources/codeList.xml#MD_MaintenanceFrequencyCode"
                                                            codeListValue="notPlanned">Sin planificar</gmd:MD_MaintenanceFrequencyCode>
                        </gmd:maintenanceAndUpdateFrequency>
                    </gmd:MD_MaintenanceInformation>
                </gmd:resourceMaintenance>

                <gmd:descriptiveKeywords>
                    <gmd:MD_Keywords>
                        {eiiob_pkey}
                    </gmd:MD_Keywords>
                </gmd:descriptiveKeywords>
                
                <gmd:descriptiveKeywords>
                    <gmd:MD_Keywords>
                        <gmd:keyword>
                        <gco:CharacterString>{eiiob_inspire}</gco:CharacterString>
                        </gmd:keyword>
                        <gmd:thesaurusName>
                        <gmd:CI_Citation>
                            <gmd:title>
                            <gco:CharacterString>Temas INSPIRE</gco:CharacterString>
                            </gmd:title>
                        </gmd:CI_Citation>
                        </gmd:thesaurusName>
                    </gmd:MD_Keywords>
        </gmd:descriptiveKeywords>

                <gmd:language>
                    <gmd:LanguageCode codeList="http://www.loc.gov/standards/iso639-2/" 
                                    codeListValue="spa">spa</gmd:LanguageCode>
                </gmd:language>

                <gmd:topicCategory>
                    <gco:CharacterString>{eiiob_categoria}</gco:CharacterString>
                </gmd:topicCategory>

                <gmd:spatialRepresentationType>
                    <gmd:MD_SpatialRepresentationTypeCode 
                        codeList="http://www.isotc211.org/2005/resources/codeList.xml#MD_SpatialRepresentationTypeCode"
                        codeListValue="{eiiob_representacion}">{eiiob_representacion}</gmd:MD_SpatialRepresentationTypeCode>
                </gmd:spatialRepresentationType>


                <gmd:referenceSystemInfo>
                    <gmd:MD_ReferenceSystem>
                        <gmd:referenceSystemIdentifier>
                            <gmd:RS_Identifier>
                                <gmd:code>
                                    <gco:CharacterString>{eiiob_referencia}</gco:CharacterString>
                                </gmd:code>
                            </gmd:RS_Identifier>
                        </gmd:referenceSystemIdentifier>
                    </gmd:MD_ReferenceSystem>
                </gmd:referenceSystemInfo>

                <srv:serviceType>
                    <gco:LocalName codeSpace="www.w3c.org">OGC:WMS</gco:LocalName>
                </srv:serviceType>
                <srv:serviceTypeVersion>
                    <gco:CharacterString>1.3.0</gco:CharacterString>
                </srv:serviceTypeVersion>
                <srv:extent>
                    <gmd:EX_Extent xmlns:xs="http://www.w3.org/2001/XMLSchema">
                    <gmd:geographicElement>
                        <gmd:EX_GeographicBoundingBox>
                        <gmd:westBoundLongitude>
                            <gco:Decimal>{min_longitud}</gco:Decimal>
                        </gmd:westBoundLongitude>
                        <gmd:eastBoundLongitude>
                            <gco:Decimal>{max_longitud}</gco:Decimal>
                        </gmd:eastBoundLongitude>
                        <gmd:southBoundLatitude>
                            <gco:Decimal>{min_latitud}</gco:Decimal>
                        </gmd:southBoundLatitude>
                        <gmd:northBoundLatitude>
                            <gco:Decimal>{max_latitud}</gco:Decimal>
                        </gmd:northBoundLatitude>
                        </gmd:EX_GeographicBoundingBox>
                    </gmd:geographicElement>
                    </gmd:EX_Extent>
                </srv:extent>

                <gmd:presentationForm>
                    <gco:CharacterString>Modelo Digital</gco:CharacterString>
                </gmd:presentationForm>

                <srv:couplingType>
                    <srv:SV_CouplingType codeListValue="tight"
                                        codeList="http://www.isotc211.org/2005/iso19119/resources/Codelist/gmxCodelists.xml#SV_CouplingType"/>
                </srv:couplingType>


            </srv:SV_ServiceIdentification>
        </gmd:identificationInfo>
                
        
        <gmd:distributionInfo>
            <gmd:MD_Distribution>
                <gmd:distributor>
                    <gmd:MD_Distributor>
                    <gmd:distributorContact>
                        <gmd:CI_ResponsibleParty>
                            <gmd:individualName>
                                <gco:CharacterString>I+D</gco:CharacterString>
                            </gmd:individualName>
                            <gmd:organisationName>
                                <gco:CharacterString>Avincis</gco:CharacterString>
                            </gmd:organisationName>
                            <gmd:contactInfo>
                                <gmd:CI_Contact>
                                <gmd:address>
                                    <gmd:CI_Address>
                                        <gmd:electronicMailAddress>
                                            <gco:CharacterString>soporte@einforex.es</gco:CharacterString>
                                        </gmd:electronicMailAddress>
                                    </gmd:CI_Address>
                                </gmd:address>
                                <gmd:onlineResource>
                                    <gmd:CI_OnlineResource>
                                        <gmd:linkage>
                                            <gmd:URL>https://www.avincis.com</gmd:URL>
                                        </gmd:linkage>
                                        <gmd:protocol gco:nilReason="missing">
                                            <gco:CharacterString/>
                                        </gmd:protocol>
                                        <gmd:name gco:nilReason="missing">
                                            <gco:CharacterString/>
                                        </gmd:name>
                                        <gmd:description gco:nilReason="missing">
                                            <gco:CharacterString/>
                                        </gmd:description>
                                    </gmd:CI_OnlineResource>
                                </gmd:onlineResource>
                                </gmd:CI_Contact>
                            </gmd:contactInfo>
                            <gmd:role>
                                <gmd:CI_RoleCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_RoleCode"
                                                codeListValue="distributor"/>
                            </gmd:role>
                        </gmd:CI_ResponsibleParty>
                    </gmd:distributorContact>
                    </gmd:MD_Distributor>
                </gmd:distributor>
                
                <gmd:transferOptions>
                    <gmd:MD_DigitalTransferOptions>
                      {gmd_online_resources}
                    </gmd:MD_DigitalTransferOptions>
                </gmd:transferOptions>

                
            </gmd:MD_Distribution>
        </gmd:distributionInfo>
        

        <gmd:dataQualityInfo>
                <gmd:DQ_DataQuality>
                    <gmd:scope>
                        <gmd:DQ_Scope>
                        <gmd:level>
                            <gmd:MD_ScopeCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_ScopeCode"
                                                codeListValue="service"/>
                        </gmd:level>
                        </gmd:DQ_Scope>
                    </gmd:scope>
                    <gmd:lineage>
                        <gmd:LI_Lineage/>
                    </gmd:lineage>
                </gmd:DQ_DataQuality>
        </gmd:dataQualityInfo>
        <gmd:applicationSchemaInfo>
                <gmd:MD_ApplicationSchemaInformation>
                    <gmd:name>
                        <gmd:CI_Citation>
                        <gmd:title gco:nilReason="missing">
                            <gco:CharacterString/>
                        </gmd:title>
                        <gmd:date>
                            <gmd:CI_Date>
                                <gmd:date>
                                    <gco:Date/>
                                </gmd:date>
                                <gmd:dateType>
                                    <gmd:CI_DateTypeCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_DateTypeCode"
                                                        codeListValue=""/>
                                </gmd:dateType>
                            </gmd:CI_Date>
                        </gmd:date>
                        </gmd:CI_Citation>
                    </gmd:name>
                    <gmd:schemaLanguage gco:nilReason="missing">
                        <gco:CharacterString/>
                    </gmd:schemaLanguage>
                    <gmd:constraintLanguage gco:nilReason="missing">
                        <gco:CharacterString/>
                    </gmd:constraintLanguage>
                </gmd:MD_ApplicationSchemaInformation>
            </gmd:applicationSchemaInfo>
        </gmd:MD_Metadata>
"""
    xml_encoded = base64.b64encode(xml.encode('utf-8')).decode('utf-8')
    return xml_encoded



def generar_gmd_online(wms_layers_info):
    online_blocks = ""
    for wms in wms_layers_info:
        online_blocks += f"""
            <gmd:onLine>
                <gmd:CI_OnlineResource>
                    <gmd:linkage>
                        <gmd:URL>{escape(wms['wms_server'])}</gmd:URL>
                    </gmd:linkage>
                    <gmd:protocol>
                        <gco:CharacterString>OGC:WMS</gco:CharacterString>
                    </gmd:protocol>
                    <gmd:name>
                        <gco:CharacterString>{escape(wms['wms_layer'])}</gco:CharacterString>
                    </gmd:name>
                    <gmd:description>
                        <gco:CharacterString>{escape(wms['wms_description_tiff'])}</gco:CharacterString>
                    </gmd:description>
                    <gmd:function>
                        <gmd:CI_OnLineFunctionCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                   codeListValue="download"/>
                    </gmd:function>
                </gmd:CI_OnlineResource>
            </gmd:onLine>
        """
    return online_blocks




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
    