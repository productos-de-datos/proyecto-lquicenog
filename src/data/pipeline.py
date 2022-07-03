"""
Documentación:
Este pipeline tiene como objetivo orquestar todas las funciones anteriormente realizadas, una vez se haya creado el datalake. 
Es decir, que se ejecutan las funciones: ingest_data, transform_data, clean_data, compute_daily_prices and compute_monthly_prices. 
"""


"""
Construya un pipeline de Luigi que:

* Importe los datos xls
* Transforme los datos xls a csv
* Cree la tabla unica de precios horarios.
* Calcule los precios promedios diarios
* Calcule los precios promedios mensuales

En luigi llame las funciones que ya creo.


"""

import luigi
from luigi import Task, LocalTarget

#Se cre una clase para llamar la función de importar datos (ingest_data)
class ingestar_datos(Task):
    def output(self):
        return LocalTarget('data_lake/landing/ingestar_datos_pipeline.txt')

    def run(self):

        from ingest_data import ingest_data
        with self.output().open('w') as archivos:
            ingest_data()

#Se crea una clase para llamar la función que transforma los datos de xls y xlsx a csv (transform_data)
class transformar_datos(Task):
    def requires(self):
        return ingestar_datos()

    def output(self):
        return LocalTarget('data_lake/raw/transformar_datos_pipeline.txt')

    def run(self):

        from transform_data import transform_data
        with self.output().open('w') as archivos:
            transform_data()

#Se crea una clase para llamar la función clean_data y crear un solo archivo tipo csv 
class limpiar_datos(Task):
    def requires(self):
        return transformar_datos()

    def output(self):
        return LocalTarget('data_lake/cleansed/limpiar_datos_pipeline.txt')

    def run(self):

        from clean_data import clean_data
        with self.output().open('w') as archivos:
            clean_data()

#Se crea una clase para llamar la función compute_daily_prices y calcular el promedio de precios diario
class precio_diario(Task):
    def requires(self):
        return limpiar_datos()

    def output(self):
        return LocalTarget('data_lake/business/precio_diario_pipeline.txt')

    def run(self):

        from compute_daily_prices import compute_daily_prices
        with self.output().open('w') as archivos:
            compute_daily_prices()

#Se crea una clase para llamar la función compute_monthly_prices y calcular el promedio mensual de precios.
    def requires(self):
        return precio_diario()

    def output(self):
        return LocalTarget('data_lake/business/precio_mensual_pipeline.txt')

    def run(self):

        from compute_monthly_prices import compute_monthly_prices
        with self.output().open('w') as archivos:
            compute_monthly_prices()


if __name__ == '__main__':
    luigi.run(["precio_mensual", "--local-scheduler"])

if __name__ == "__main__":
    import doctest

    doctest.testmod()
