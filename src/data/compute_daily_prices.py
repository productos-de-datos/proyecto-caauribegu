"""
Computación de los precios diarios
----------------------------------------------------------------------------------------------------------

El objetivo de este módulo es presentar los calculos del precio promedio por día mediante la asignación 
de la columna fecha en su respectivo formato. Se almacena la inforamción en el archivo precios-diarios.csv.




"""


def compute_daily_prices():
    """Compute los precios promedios diarios.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio diario (sobre las 24 horas del dia) para cada uno de los dias. Las
    columnas del archivo data_lake/business/precios-diarios.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio diario de la electricidad en la bolsa nacional



    """
    # raise NotImplementedError("Implementar esta función")

    import pandas as pd

    precios_diarios = pd.read_csv('data_lake/cleansed/precios-horarios.csv')
    precios_diarios['fecha'] = pd.to_datetime(
        precios_diarios['fecha'], format="%Y/%m/%d")
    precios_diarios = precios_diarios.set_index('fecha')

    precios_diarios = precios_diarios.resample('D').mean()
    precios_diarios = precios_diarios.reset_index()
    precios_diarios = precios_diarios.iloc[:, [0, 2]]

    precios_diarios.to_csv(
        'data_lake/business/precios-diarios.csv', encoding='utf-8', index=False)


def test_answer():
    import os

    assert os.path.isfile('data_lake/business/precios-diarios.csv') is True


if __name__ == "__main__":
    import doctest

    doctest.testmod()

    compute_daily_prices()
