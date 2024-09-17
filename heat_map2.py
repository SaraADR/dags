# Ruta al archivo TIFF que se va a subir a MinIO
import gdal, osr
import os



TIFF = './dags/repo/recursos/tests-geonetwork-_0026_4740004_611271.tif'
TIFF2 = './dags/repo/recursos/proyeccioncambiada.tif'


def cambiar_proyeccion_tiff(input_tiff, output_tiff):
    # Abrir el archivo TIFF
    dataset = gdal.Open(input_tiff, gdal.GA_Update)

    if dataset is None:
        raise FileNotFoundError(f"No se pudo abrir el archivo TIFF: {input_tiff}")

    # Obtener la proyección actual
    proyeccion = dataset.GetProjection()

    # Crear un objeto SpatialReference
    srs = osr.SpatialReference()

    # Revisar si ya tiene proyección
    if proyeccion:
        print(f"Proyección actual: {proyeccion}")

        # Si ya es EPSG:3857, no se cambia
        srs.ImportFromWkt(proyeccion)
        if srs.IsProjected() and srs.GetAttrValue("AUTHORITY", 1) == '3857':
            print("El archivo ya tiene la proyección EPSG:3857.")
        else:
            # Cambiar proyección a EPSG:3857
            print("Cambiando la proyección a EPSG:3857.")
            srs.ImportFromEPSG(3857)
            dataset.SetProjection(srs.ExportToWkt())
    else:
        # Si no tiene proyección, aplicar EPSG:3857
        print("No tiene proyección, aplicando EPSG:3857.")
        srs.ImportFromEPSG(3857)
        dataset.SetProjection(srs.ExportToWkt())

    # Guardar el archivo TIFF con la nueva proyección
    gdal.Warp(output_tiff, dataset, dstSRS='EPSG:3857')

    # Cerrar el dataset
    dataset = None

    print(f"Se ha guardado el archivo con la proyección EPSG:3857 en: {output_tiff}")

cambiar_proyeccion_tiff(input_tiff=TIFF,output_tiff=TIFF2)
