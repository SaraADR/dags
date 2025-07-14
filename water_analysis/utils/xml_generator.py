import base64
from datetime import datetime, timedelta, timezone
import geopandas as gpd
from xml.sax.saxutils import escape
from pyproj import Transformer


def generate_dynamic_xml(json_modificado, layer_name, workspace, base_url,uuid_key, coordenadas_tif, wms_server_tiff, wms_layer_tiff, wms_description_tiff, id_mission, url_new,  ruta_png, ruta_pdf, ruta_csv, ruta_tiff, zip_file_save, wms_wfs):

    descripcion = "Resultado del algoritmo de waterAnalysis"

    base_url_sinrest = base_url.replace('/rest/', '/')

    water_layer_name = next((k for k in wms_wfs if "water" in k.lower()), None)
    wms_info_water = wms_wfs.get(water_layer_name, {})

    wms_server_shp_water = escape(wms_info_water.get("wms_server", "").replace('/rest/', '/')) 
    wfs_server_shp_water = escape(wms_info_water.get("wfs_server", "").replace('/rest/', '/'))
    wms_layer_shp_water = wms_info_water.get("wms_layer", "")
    wfs_layer_shp_water = wms_info_water.get("wfs_layer", "")
    wms_description_shp_water = wms_info_water.get("wms_description", "")
    wfs_description_shp_water = wms_info_water.get("wfs_description", "")
    # Buscar la info de 'seafloor'
    seafloor_layer_name = next((k for k in wms_wfs if "seafloor" in k.lower()), None)
    wms_info_seafloor = wms_wfs.get(seafloor_layer_name, {})

    wms_server_shp_seafloor = escape(wms_info_seafloor.get("wms_server", "").replace('/rest/', '/')) 
    wfs_server_shp_seafloor = escape(wms_info_seafloor.get("wfs_server", "").replace('/rest/', '/'))
    wms_layer_shp_seafloor = wms_info_seafloor.get("wms_layer", "")
    wfs_layer_shp_seafloor = wms_info_seafloor.get("wfs_layer", "")
    wms_description_shp_seafloor = wms_info_seafloor.get("wms_description", "")
    wfs_description_shp_seafloor = wms_info_seafloor.get("wfs_description", "")


    wms_server_tiff_escape = escape(wms_server_tiff.replace('/rest/', '/'))


    for metadata in json_modificado['metadata']:
        if metadata['name'] == 'ExecutionID':
            file_identifier = metadata['value']

    titulo = "WaterAnalysis: " + datetime.now().strftime('%Y%m%d_%H%M%S')
     
    fecha_completa = datetime.strptime(json_modificado['endTimestamp'], "%Y%m%dT%H%M%S")
    fecha = fecha_completa.date()


    transformer = Transformer.from_crs("EPSG:25829", "EPSG:4326", always_xy=True)

    min_longitud = coordenadas_tif["min_longitud"]
    max_longitud = coordenadas_tif["max_longitud"]
    min_latitud = coordenadas_tif["min_latitud"]
    max_latitud = coordenadas_tif["max_latitud"]

    min_longitud, min_latitud = transformer.transform(min_longitud, min_latitud)
    max_longitud, max_latitud = transformer.transform(max_longitud, max_latitud)


    print(min_longitud, min_latitud , max_latitud, max_longitud)

    # min_longitud = "-7.6392"
    # max_longitud = "-7.6336"
    # min_latitud = "42.8025"
    # max_latitud = "42.8044"

    informe_description = 'Informe generado por el algoritmo'
    csv_description = 'Csv generado por el algoritmo'
    tif_description = 'Tiff generado por el algoritmo'
    seafloor_zip_description = 'Zip con los datos generados para Seafloor por parte del algoritmo'


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
            <gco:DateTime>${fecha_completa}</gco:DateTime>
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
                        <gco:CharacterString>{titulo}</gco:CharacterString>
                    </gmd:title>
                    <gmd:date>
                        <gmd:CI_Date>
                            <gmd:date>
                                <gco:Date>${fecha}</gco:Date>
                            </gmd:date>
                            <gmd:dateType>
                                <gmd:CI_DateTypeCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_DateTypeCode"
                                                    codeListValue="publication"/>
                            </gmd:dateType>
                        </gmd:CI_Date>
                    </gmd:date>
                    </gmd:CI_Citation>
                </gmd:citation>

              <gmd:graphicOverview>
                <gmd:MD_BrowseGraphic>
                    <gmd:fileName>
                    <gco:CharacterString>
                        {ruta_png}
                    </gco:CharacterString>
                    </gmd:fileName>
                </gmd:MD_BrowseGraphic>
                </gmd:graphicOverview>

                <gmd:abstract>
                    <gco:CharacterString>{descripcion}</gco:CharacterString>
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
                <gmd:resourceMaintenance/>

                <gmd:descriptiveKeywords>
                    <gmd:MD_Keywords>
                    <gmd:keyword>
                        <gco:CharacterString>WMS</gco:CharacterString>
                    </gmd:keyword>
                    <gmd:keyword>
                        <gco:CharacterString>WFS</gco:CharacterString>
                    </gmd:keyword>
                    <gmd:keyword>
                        <gco:CharacterString>Biodiversidad</gco:CharacterString>
                    </gmd:keyword>
                    <gmd:keyword>
                        <gco:CharacterString>Inspección acuática</gco:CharacterString>
                    </gmd:keyword>
                    <gmd:keyword>
                        <gco:CharacterString>USV</gco:CharacterString>
                    </gmd:keyword>
                    </gmd:MD_Keywords>
                </gmd:descriptiveKeywords>
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

                                        <gmd:onLine>
                        <gmd:CI_OnlineResource>
                            <gmd:linkage>
                                <gmd:URL>{wms_server_shp_seafloor}</gmd:URL>
                            </gmd:linkage>
                            <gmd:protocol>
                                <gco:CharacterString>OGC:WMS</gco:CharacterString>
                            </gmd:protocol>
                            <gmd:name>
                                <gco:CharacterString>{wms_layer_shp_seafloor}</gco:CharacterString>
                            </gmd:name>
                            <gmd:description>
                                <gco:CharacterString>{wms_description_shp_seafloor}</gco:CharacterString>
                            </gmd:description>
                            <gmd:function>
                                <gmd:CI_OnLineFunctionCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                        codeListValue="download"/>
                            </gmd:function>
                        </gmd:CI_OnlineResource>
                    </gmd:onLine>

                    <gmd:onLine>
                        <gmd:CI_OnlineResource>
                            <gmd:linkage>
                                <gmd:URL>{wfs_server_shp_seafloor}</gmd:URL>
                            </gmd:linkage>
                            <gmd:protocol>
                                <gco:CharacterString>OGC:WMS</gco:CharacterString>
                            </gmd:protocol>
                            <gmd:name>
                                <gco:CharacterString>{wfs_layer_shp_seafloor}</gco:CharacterString>
                            </gmd:name>
                            <gmd:description>
                                <gco:CharacterString>{wfs_description_shp_seafloor}</gco:CharacterString>
                            </gmd:description>
                            <gmd:function>
                                <gmd:CI_OnLineFunctionCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                        codeListValue="download"/>
                            </gmd:function>
                        </gmd:CI_OnlineResource>
                    </gmd:onLine>

                    <gmd:onLine>
                        <gmd:CI_OnlineResource>
                            <gmd:linkage>
                                <gmd:URL>{wms_server_tiff_escape}</gmd:URL>
                            </gmd:linkage>
                            <gmd:protocol>
                                <gco:CharacterString>OGC:WMS</gco:CharacterString>
                            </gmd:protocol>
                            <gmd:name>
                                <gco:CharacterString>{wms_layer_tiff}</gco:CharacterString>
                            </gmd:name>
                            <gmd:description>
                                <gco:CharacterString>{wms_description_tiff}</gco:CharacterString>
                            </gmd:description>
                            <gmd:function>
                                <gmd:CI_OnLineFunctionCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                        codeListValue="download"/>
                            </gmd:function>
                        </gmd:CI_OnlineResource>
                    </gmd:onLine>

                    <gmd:onLine>
                        <gmd:CI_OnlineResource>
                            <gmd:linkage>
                                <gmd:URL>{wms_server_shp_water}</gmd:URL>
                            </gmd:linkage>
                            <gmd:protocol>
                                <gco:CharacterString>OGC:WMS</gco:CharacterString>
                            </gmd:protocol>
                            <gmd:name>
                                <gco:CharacterString>{wms_layer_shp_water}</gco:CharacterString>
                            </gmd:name>
                            <gmd:description>
                                <gco:CharacterString>{wms_description_shp_water}</gco:CharacterString>
                            </gmd:description>
                            <gmd:function>
                                <gmd:CI_OnLineFunctionCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                        codeListValue="information"/>
                            </gmd:function>
                        </gmd:CI_OnlineResource>
                    </gmd:onLine>
                    <gmd:onLine>
                        <gmd:CI_OnlineResource>
                            <gmd:linkage>
                                <gmd:URL>{wfs_server_shp_water}</gmd:URL>
                            </gmd:linkage>
                            <gmd:protocol>
                                <gco:CharacterString>OGC:WFS-1.0.0-http-get-capabilities</gco:CharacterString>
                            </gmd:protocol>
                            <gmd:name>
                                <gco:CharacterString>{wms_layer_shp_water}</gco:CharacterString>
                            </gmd:name>
                            <gmd:description>
                                <gco:CharacterString>{wfs_description_shp_water}</gco:CharacterString>
                            </gmd:description>
                            <gmd:function>
                                <gmd:CI_OnLineFunctionCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                        codeListValue="download"/>
                            </gmd:function>
                        </gmd:CI_OnlineResource>
                    </gmd:onLine>





                    <gmd:onLine>
                        <gmd:CI_OnlineResource>
                            <gmd:linkage>
                                <gmd:URL>{ruta_pdf}</gmd:URL>
                            </gmd:linkage>
                            <gmd:protocol>
                                <gco:CharacterString>WWW:DOWNLOAD-1.0-http--download</gco:CharacterString>
                            </gmd:protocol>
                            <gmd:name>
                                <gco:CharacterString>{informe_description}</gco:CharacterString>
                            </gmd:name>
                            <gmd:function>
                                <gmd:CI_OnLineFunctionCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                        codeListValue="download"/>
                            </gmd:function>
                        </gmd:CI_OnlineResource>
                    </gmd:onLine>
                    <gmd:onLine>
                        <gmd:CI_OnlineResource>
                            <gmd:linkage>
                                <gmd:URL>{ruta_csv}</gmd:URL>
                            </gmd:linkage>
                            <gmd:protocol>
                                <gco:CharacterString>WWW:DOWNLOAD-1.0-http--download</gco:CharacterString>
                            </gmd:protocol>
                            <gmd:name>
                                <gco:CharacterString>{csv_description}</gco:CharacterString>
                            </gmd:name>
                            <gmd:function>
                                <gmd:CI_OnLineFunctionCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                        codeListValue="download"/>
                            </gmd:function>
                        </gmd:CI_OnlineResource>
                    </gmd:onLine>
                    <gmd:onLine>
                        <gmd:CI_OnlineResource>
                            <gmd:linkage>
                                <gmd:URL>{ruta_tiff}</gmd:URL>
                            </gmd:linkage>
                            <gmd:protocol>
                                <gco:CharacterString>WWW:DOWNLOAD-1.0-http--download</gco:CharacterString>
                            </gmd:protocol>
                            <gmd:name>
                                <gco:CharacterString>{tif_description}</gco:CharacterString>
                            </gmd:name>
                            <gmd:function>
                                <gmd:CI_OnLineFunctionCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                        codeListValue="download"/>
                            </gmd:function>
                        </gmd:CI_OnlineResource>
                    </gmd:onLine>
                                        <gmd:onLine>
                        <gmd:CI_OnlineResource>
                            <gmd:linkage>
                                <gmd:URL>{zip_file_save}</gmd:URL>
                            </gmd:linkage>
                            <gmd:protocol>
                                <gco:CharacterString>WWW:DOWNLOAD-1.0-http--download</gco:CharacterString>
                            </gmd:protocol>
                            <gmd:name>
                                <gco:CharacterString>{seafloor_zip_description}</gco:CharacterString>
                            </gmd:name>
                            <gmd:function>
                                <gmd:CI_OnLineFunctionCode codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                        codeListValue="download"/>
                            </gmd:function>
                        </gmd:CI_OnlineResource>
                    </gmd:onLine>

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
    # print(xml)
    xml_encoded = base64.b64encode(xml.encode('utf-8')).decode('utf-8')
    return xml_encoded