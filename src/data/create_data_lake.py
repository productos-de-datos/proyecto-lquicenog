"""
Documentación:
La funcionalidad de create_data_lake es la creación y jerarquía del directorio y sus carpetas
"""

def create_data_lake():
    """Cree el data lake con sus capas.

    Esta función debe crear la carpeta `data_lake` en la raiz del proyecto. El data lake contiene
    las siguientes subcarpetas:

    ```
    .
    |
    \___ data_lake/
         |___ landing/
         |___ raw/
         |___ cleansed/
         \___ business/
              |___ reports/
              |    |___ figures/
              |___ features/
              |___ forecasts/

    ```


    
    """
    #Creamos carpetas  usando la funcion mkdir del modulo os
    import os

    os.mkdir('./data_lake/')
    parent_dir = 'data_lake/'
    carpetas = ['landing', 'raw', 'cleansed', 'business']
    [os.mkdir(os.path.join(parent_dir, c)) for c in carpetas]
    parent_dir = 'data_lake/business/'
    carpetas = ['reports', 'features', 'forecasts']
    [os.mkdir(os.path.join(parent_dir, c)) for c in carpetas]
    parent_dir = 'data_lake/business/reports/'
    directory = 'figures'
    os.mkdir(os.path.join(parent_dir, directory))
    

    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    create_data_lake()


