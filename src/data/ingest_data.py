"""
Módulo de ingestión de datos.
-------------------------------------------------------------------------------
"""

import queue


def ingest_data():
    """Ingeste los datos externos a la capa landing del data lake.
    Del repositorio jdvelasq/datalabs/precio_bolsa_nacional/xls/ descarge los
    archivos de precios de bolsa nacional en formato xls a la capa landing. La
    descarga debe realizarse usando únicamente funciones de Python.
    """


    # La función a continuación busca generar la ingesta de los datos a la carpeta datalake/datalanding que se crea con la función anterior


    # La función se encarga de descargar los archivos consecutivamente teniendo en cuenta los años de creación de cada uno y utilizando
    # un ciclo for y la libreria requests para poder realizar está tarea. De igual forma se abren los archivos xls y se accede con wget.


    import requests as req

    for i in range(1995, 2022):
        if i in range(2016, 2018):
            url = 'https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{}.xls?raw=true'.format(i)
            file = req.get(url, allow_redirects=True)
            open('data_lake/landing/{}.xls'.format(i), 'wb').write(file.content)
        else:
            url = 'https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{}.xlsx?raw=true'.format(i)
            file = req.get(url, allow_redirects=True)
            open('data_lake/landing/{}.xlsx'.format(i), 'wb').write(file.content)

    #raise NotImplementedError("Implementar esta función")

if __name__ == "__main__":
    import doctest

    doctest.testmod()

ingest_data()