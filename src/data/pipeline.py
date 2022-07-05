"""
Construya un pipeline de Luigi que:

* Importe los datos xls
* Transforme los datos xls a csv
* Cree la tabla unica de precios horarios.
* Calcule los precios promedios diarios
* Calcule los precios promedios mensuales

La idea de usar la libreria Luigi es llamar las funciones creadas. 


"""
import luigi
from luigi import Task, LocalTarget


class IngestarData(Task):
    def output(self):
        return LocalTarget('data_lake/Luigi01_IngestarData.txt')

    def run(self):
        from ingest_data import ingest_data
        with self.output().open('w') as archivos:
            ingest_data()


class TransformarData(Task):
    def requires(self):
        return IngestarData()

    def output(self):
        return LocalTarget('data_lake/Luigi02_TransformarData.txt')

    def run(self):
        from transform_data import transform_data
        with self.output().open('w') as archivos:
            transform_data()


class LimpiarData(Task):
    def requires(self):
        return TransformarData()

    def output(self):
        return LocalTarget('data_lake/Luigi03_LimpiarData.txt')

    def run(self):
        from clean_data import clean_data
        with self.output().open('w') as archivos:
            clean_data()


class ComputarPrecioDiario(Task):
    def requires(self):
        return LimpiarData()

    def output(self):
        return LocalTarget('data_lake/Luigi04_ComputarPrecioDiario.txt')

    def run(self):
        from compute_daily_prices import compute_daily_prices
        with self.output().open('w') as archivos:
            compute_daily_prices()


class ComputarPrecioMensual(Task):
    def requires(self):
        return ComputarPrecioDiario()

    def output(self):
        return LocalTarget('data_lake/Luigi05_ComputarPrecioMensual.txt')

    def run(self):
        from compute_monthly_prices import compute_monthly_prices
        with self.output().open('w') as archivos:
            compute_monthly_prices()


if __name__ == "__main__":

    luigi.run(["ComputarPrecioMensual", "--local-scheduler"])

    #raise NotImplementedError("Implementar esta función")

if __name__ == "__main__":
    import doctest

    doctest.testmod()
