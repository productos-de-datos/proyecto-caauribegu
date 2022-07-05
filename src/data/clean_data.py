from numpy import true_divide


def clean_data():
    """Realice la limpieza y transformación de los archivos CSV.

    Usando los archivos data_lake/raw/*.csv, cree el archivo data_lake/cleansed/precios-horarios.csv.
    Las columnas de este archivo son:

    * fecha: fecha en formato YYYY-MM-DD
    * hora: hora en formato HH
    * precio: precio de la electricidad en la bolsa nacional

    Este archivo contiene toda la información del 1997 a 2021.


    """
    # raise NotImplementedError("Implementar esta función")

    import glob
    import os
    import pandas as pd

    # Primero se unen todas los archivos utilizando la siguiente función:

    total_files = os.path.join('data_lake/raw/', '*.csv')

    # Ahora se realiza una lista global de total_files es decir que quda [1997.csv, 1998.csv, ... , 2021.csv]

    files_list = glob.glob(total_files)

    precios_horarios = pd.concat(
        map(pd.read_csv, files_list), ignore_index=True)

    precios_horarios['fecha'] = pd.to_datetime(
        precios_horarios['fecha'], format='%Y-%m-%d')

    precios_horarios = precios_horarios.where(precios_horarios.notna(
    ), precios_horarios.mean(axis=1, numeric_only=True), axis=0)
    precios_horarios = precios_horarios.drop_duplicates(
        ['fecha'], keep='first')
    precios_horarios = pd.melt(precios_horarios, id_vars="fecha")
    precios_horarios = precios_horarios.sort_values(by=['fecha', 'variable'])
    precios_horarios.rename(columns={'variable': 'hora',
                            'value': 'precio'}, inplace=True)

    precios_horarios.to_csv(
        'data_lake/cleansed/precios-horarios.csv', encoding='utf-8', index=False)


if __name__ == "__main__":
    import doctest

    doctest.testmod()

    clean_data()
