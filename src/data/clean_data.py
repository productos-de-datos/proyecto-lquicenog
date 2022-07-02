def clean_data():
    """Realice la limpieza y transformación de los archivos CSV.

    Usando los archivos data_lake/raw/*.csv, cree el archivo data_lake/cleansed/precios-horarios.csv.
    Las columnas de este archivo son:

    * fecha: fecha en formato YYYY-MM-DD
    * hora: hora en formato HH
    * precio: precio de la electricidad en la bolsa nacional

    Este archivo contiene toda la información del 1997 a 2021.


    """

    import pandas as pd
    import glob

    #Usamos la funcion glob para abrir el paquete de archivos con extension .csv
    path_file = glob.glob(r'data_lake/raw/*.csv')
    li = []

    #Hacemos un ciclo para recorrer cada archivo en el paquete
    for filename in path_file:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df) #Se adiciona cada archivo leido
    
    #Obtenemos un dataframe con toda la data proveniente de los archivos raw, pero con las columas de
    #Feche, H00, ..... H23
    read_file = pd.concat(li, axis=0, ignore_index=True)
    #print(read_file.tail())
    read_file = read_file[read_file["Fecha"].notnull()]
    
    #Sacamos la columna solo con las fechas
    fechas = read_file.iloc[:, 0] 
    
    #Organizamos los datos para obtener las 3 columnas de interes fecha, hora y precio
    lista_datos = []
    precio = 0
    contador_filas = 0

    for fecha in fechas:
        for hora in range(0, 24):
            precio = (read_file.iloc[contador_filas, (hora+1)])
            lista_datos.append([fecha, hora, precio])
        contador_filas += 1

    df = pd.DataFrame(lista_datos, columns=["fecha", "hora", "precio"])
    df = df[df["precio"].notnull()]

    df.to_csv("data_lake/cleansed/precios-horarios.csv", index=None, header=True)

    
    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    clean_data()




