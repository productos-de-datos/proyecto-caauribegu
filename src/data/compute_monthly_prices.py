"""
Cálculo de los precios mensuales.

    La principal idea de esta función es generar un reporte que compute mensualmente el promedio del precio.
    Lo anterior se hace leyendo el archivo csv con la información de los precios horarios. Se organizan en fechas
    y se calcula el rpomedio para el mes. ASignandouna columna de precio. Finalmente se guarda en un archivo de salida csv.



"""


def compute_monthly_prices():
    """Compute los precios promedios mensuales.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio mensual. Las
    columnas del archivo data_lake/business/precios-mensuales.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio mensual de la electricidad en la bolsa nacional



    """
    # raise NotImplementedError("Implementar esta función")

    import pandas as pd

    precios_horarios = pd.read_csv('data_lake/cleansed/precios-horarios.csv')
    precios_horarios[['ano', 'mes', 'dia']
                     ] = precios_horarios['fecha'].str.split('-', expand=True)
    precios_horarios['fecha2'] = precios_horarios['ano'] + \
        '-' + precios_horarios['mes'] + '-01'
    precios_horarios = precios_horarios.drop(
        ['fecha', 'ano', 'mes', 'dia', 'hora'], axis=1)
    precios_mensuales = precios_horarios.groupby(['fecha2'])['precio'].mean()
    precios_mensuales = precios_mensuales.reset_index()
    precios_mensuales = precios_mensuales.rename(columns={'fecha2': 'fecha'})
    precios_mensuales.to_csv(
        'data_lake/business/precios-mensuales.csv', index=False,  encoding='utf-8')


def test_answer():
    import os

    assert os.path.isfile('data_lake/business/precios-mensuales.csv') is True


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    compute_monthly_prices()
