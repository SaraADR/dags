import base64
from datetime import datetime, timedelta, timezone
from dag_utils import upload_to_minio_path, get_geoserver_connection, get_minio_client, execute_query
import os
import requests
import geopandas as gpd
from pyproj import Transformer
import logging
import zipfile
import io

def subir_zip_shapefile(file_group, nombre_capa, WORKSPACE, base_url, auth):
    if not file_group:
        return
    
    required_extensions = [".shp", ".dbf", ".shx"]
    presentes = {os.path.splitext(name)[1].lower(): path for name, path in file_group}

    for ext in required_extensions:
        if ext not in presentes:
            logging.warning(f"⚠️ Falta {ext} en {nombre_capa}. Esto podría causar errores.")
    
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        for nombre_original, ruta_temporal in file_group:
            with open(ruta_temporal, 'rb') as f:
                zip_file.writestr(nombre_original, f.read())
    zip_buffer.seek(0)

    datastore_name = f"{nombre_capa}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    url = f"{base_url}/workspaces/{WORKSPACE}/datastores/{datastore_name}/file.shp"
    headers = {"Content-type": "application/zip"}

    response = requests.put(url, headers=headers, data=zip_buffer, auth=auth, params={"configure": "all"})
    if response.status_code not in [201, 202]:
        raise Exception(f"Error subiendo vectorial {datastore_name}: {response.text}")
    print(f"Capa vectorial publicada: {datastore_name}")
    print(f"Vector disponible en: {base_url}/geoserver/{WORKSPACE}/wms?layers={WORKSPACE}:{datastore_name}")

#PUBLICAR GEO
def publish_to_geoserver(archivos, **context):
    WORKSPACE = "USV_Water_analysis_2025"
    GENERIC_LAYER = "spain_water_analysis"

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

    #SUBIMOS LOS TIFFS
    tiff_files = [path for name, path in temp_files if name.lower().endswith(".tif")]
    wms_server_tiff = None
    wms_layer_tiff = None
    wms_description_tiff = "Capa raster GeoTIFF publicada en GeoServer"

    for tif_file in tiff_files:
        with open(tif_file, 'rb') as f:
            file_data = f.read()
        
        layer_name = f"USV_Water_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        headers = {"Content-type": "image/tiff"}

        # Publicar capa raster en GeoServer
        url_new = f"{base_url}/workspaces/{WORKSPACE}/coveragestores/{layer_name}/file.geotiff"
        response = requests.put(url_new, headers=headers, data=file_data, auth=auth, params={"configure": "all"})
        if response.status_code not in [201, 202]:
            raise Exception(f"Error publicando {layer_name}: {response.text}")
        print(f"Capa raster publicada: {layer_name}")
        wms_server_tiff = f"{base_url}/{WORKSPACE}/wms"
        wms_layer_tiff = f"{WORKSPACE}:{layer_name}"

        # Actualizar capa genérica
        url_latest = f"{base_url}/workspaces/{WORKSPACE}/coveragestores/{GENERIC_LAYER}/file.geotiff"
        response_latest = requests.put(url_latest, headers=headers, data=file_data, auth=auth, params={"configure": "all"})
        if response_latest.status_code not in [201, 202]:
            raise Exception(f"Error actualizando capa genérica: {response_latest.text}")
        print(f"Capa genérica raster actualizada: {GENERIC_LAYER}")
        print(f"Raster disponible en: {base_url}/geoserver/{WORKSPACE}/wms?layers={WORKSPACE}:{layer_name}")

    #SUBIMOS LOS SHAPES
    shapefile_groups = {}
    wfs_server_shp = None
    wfs_layer_shp = None
    wfs_description_shp = "Capa vectorial publicada en GeoServer vía WFS."

    for original_name, temp_path in temp_files:
        ext = os.path.splitext(original_name)[1].lower()
        if ext in ('.shp', '.dbf', '.shx', '.prj', '.cpg'):
            base_name = os.path.splitext(original_name)[0]  
            shapefile_groups.setdefault(base_name, []).append((original_name, temp_path))

    wms_wfs = {}
    # Subir cada grupo
    for base_name, file_group in shapefile_groups.items():
        nombre_capa = os.path.basename(base_name)  
        subir_zip_shapefile(file_group, nombre_capa, WORKSPACE, base_url, auth)

        wms_server_shp = f"{base_url}/{WORKSPACE}/wms"
        wms_layer_shp = f"{WORKSPACE}:{nombre_capa}"
        wms_description_shp = f"Capa vectorial {nombre_capa} publicada en GeoServer"
        wfs_server_shp = f"{base_url}/{WORKSPACE}/ows?service=WFS&request=GetCapabilities"
        wfs_layer_shp = f"{WORKSPACE}:{nombre_capa}"
        wfs_description_shp = f"Capa wfs {nombre_capa} publicada en GeoServer"
        wms_wfs[nombre_capa] = {
            "wms_server": wms_server_shp,
            "wms_layer": wms_layer_shp,
            "wms_description": wms_description_shp,
            "wfs_server": wfs_server_shp,
            "wfs_layer": wfs_layer_shp,         
            "wfs_description": wfs_description_shp
        }
    print("----Publicación en GeoServer completada exitosamente.----")
    return layer_name, WORKSPACE, base_url, wms_server_tiff, wms_layer_tiff, wms_description_tiff, url_new, wms_wfs