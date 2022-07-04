"""
Transformación de los datos. 
----------------------------------------------------------------------------------------

    La intención de la siguiente función es convertir los documentos del datalake que se encuentran en formato excel
    a documentos tipo csv. Esto se hace a partir de el uso de lectura de datos con las funciones read xls y to csv.
    """


def transform_data():
    import pandas as pd

    """Transforme los archivos xls a csv.

    Transforme los archivos data_lake/landing/*.xls a data_lake/raw/*.csv. Hay
    un archivo CSV por cada archivo XLS en la capa landing. Cada archivo CSV
    tiene como columnas la fecha en formato YYYY-MM-DD y las horas H00, ...,
    H23.

    """

    for i in range(1995, 2022):
        if i in range(2016, 2018):
            data_xls = pd.read_excel(
                'data_lake/landing/{}.xls'.format(i), index_col=None, header=None)
            df = data_xls.dropna(axis=0, thresh=10)
            df = df.iloc[1:]
            df = df[df.columns[0:25]]
            df.columns = ["fecha", "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15",
                          "16", "17", "18", "19", "20", "21", "22", "23"]
            df["fecha"] = pd.to_datetime(df["fecha"], format="%Y/%m/%d")

            df.to_csv('data_lake/raw/{}.csv'.format(i),
                      encoding='utf-8', index=False, header=True)
        else:
            data_xls = pd.read_excel(
                'data_lake/landing/{}.xlsx'.format(i), index_col=None, header=None)
            df = data_xls.dropna(axis=0, thresh=10)
            df = df.iloc[1:]
            df = df[df.columns[0:25]]
            df.columns = ["fecha", "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15",
                          "16", "17", "18", "19", "20", "21", "22", "23"]
            df["fecha"] = pd.to_datetime(df["fecha"], format="%Y/%m/%d")
            df.to_csv('data_lake/raw/{}.csv'.format(i),
                      encoding='utf-8', index=False, header=True)

    # raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    transform_data()
