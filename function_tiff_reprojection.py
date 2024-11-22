import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling

def reproject_tiff(input_tiff, output_tiff, dst_crs='EPSG:3857'):
    """
    Reproyecta un archivo TIFF de un CRS a otro y guarda el resultado en un nuevo archivo.

    Args:
        input_tiff (str): Ruta del archivo TIFF de entrada.
        output_tiff (str): Ruta del archivo TIFF de salida.
        dst_crs (str): Sistema de referencia de coordenadas de destino (default: 'EPSG:3857').
    """
    # Abrimos el archivo TIFF original
    with rasterio.open(input_tiff) as src:
        # Calculamos la transformación y el nuevo tamaño
        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds)
        
        # Mostramos el perfil del archivo fuente
        print(src.profile)

        # Copiamos los metadatos y actualizamos con los nuevos parámetros
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': dst_crs,
            'transform': transform,
            'width': width,
            'height': height
        })    

        # Abrimos el archivo de salida para escribir
        with rasterio.open(output_tiff, 'w', **kwargs) as dst:
            # Reproyectamos cada banda
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.nearest)
        
        print(f"Reproyección completa. Archivo guardado en: {output_tiff}")

# # Ejemplo de uso
# input_tiff = './Orto_25829_1m.tif'
# output_tiff = './test3857.tif'
# reproject_tiff(input_tiff, output_tiff)













# import rasterio
# from rasterio.warp import calculate_default_transform, reproject, Resampling


# input_tiff = './Orto_25829_1m.tif'
# output_tiff = './test3857.tif'


# dst_crs = 'EPSG:3857'  

# # Abrimos el archivo TIFF original

# with rasterio.open(input_tiff) as src:

#     transform, width, height = calculate_default_transform(
#         src.crs, dst_crs, src.width, src.height, *src.bounds)
    
#     print(src.profile)

#     kwargs = src.meta.copy()
#     kwargs.update({
#         'crs': dst_crs,
#         'transform': transform,
#         'width': width,
#         'height': height
#     })    
  
#     with rasterio.open(output_tiff, 'w', **kwargs) as dst:
#         for i in range(1, src.count + 1):
#             reproject(
#                 source=rasterio.band(src, i),
#                 destination=rasterio.band(dst, i),
#                 src_transform=src.transform,
#                 src_crs=src.crs,
#                 dst_transform=transform,
#                 dst_crs=dst_crs,
#                 resampling=Resampling.nearest)
                
#     print(f"Reproyección completa. Archivo guardado en: {output_tiff}")
