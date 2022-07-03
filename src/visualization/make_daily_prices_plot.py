"""
Documentación:
    Se crea un gráfico de líneas, con el promedio de precios diarios. Se guarda un png con la imagen.
"""
def make_daily_prices_plot():
    """Crea un grafico de lines que representa los precios promedios diarios.

    Usando el archivo data_lake/business/precios-diarios.csv, crea un grafico de
    lines que representa los precios promedios diarios.

    El archivo se debe salvar en formato PNG en data_lake/business/reports/figures/daily_prices.png.

    """

    import pandas as pd
    import matplotlib.pyplot as plt

    #Cargamos el archivo de precios diarios
    path_file = r'data_lake/business/precios-diarios.csv'

    #Leemos el dataframe
    datos = pd.read_csv(path_file, index_col=None, sep=',', header=0)
    datos["fecha"] = pd.to_datetime(datos["fecha"])

    #Definimos los ejes
    x = datos.fecha
    y = datos.precio

    #Realizamos el grafico con matplotlib
    plt.figure(figsize=(15, 6))
    plt.plot(x, y, 'b', label='Precio Promedio Diario')
    plt.title('Precios Promedios Diarios')
    plt.xlabel('Fecha (Años)')
    plt.ylabel('Precio ($/kWh)')
    plt.legend()
    plt.xticks(rotation="vertical")

    #Lo guardamos en una imagen .png
    plt.savefig("data_lake/business/reports/figures/daily_prices.png")

    
    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    make_daily_prices_plot()



