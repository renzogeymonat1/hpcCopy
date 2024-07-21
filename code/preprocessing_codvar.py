import pandas as pd
import sys
import os

def preprocessing_codvar(num_fragmentos, ruta_csv):
    # Leer el archivo CSV
    df = pd.read_csv(ruta_csv)
    # Asegurarse de que la carpeta de salida exista
    carpeta_salida = f'./csv/resProcessingData/fragmentosCodVar'
    if not os.path.exists(carpeta_salida):
        os.makedirs(carpeta_salida)

    # Calcular el tamaño de cada fragmento
    total_filas = len(df)
    filas_por_fragmento = total_filas // num_fragmentos
    resto_filas = total_filas % num_fragmentos

    inicio = 0
    for i in range(num_fragmentos):
        # Calcular el índice final para este fragmento
        fin = inicio + filas_por_fragmento + (1 if i < resto_filas else 0)
        fragmento_df = df.iloc[inicio:fin]

        # Guardar el fragmento en un nuevo archivo CSV
        nombre_fragmento = f'fragmento_{i}.csv'
        ruta_fragmento = os.path.join(carpeta_salida, nombre_fragmento)
        fragmento_df.to_csv(ruta_fragmento, index=False)

        # Actualizar el índice de inicio para el siguiente fragmento
        inicio = fin

    print(f"CSV dividido en {num_fragmentos} fragmentos y guardado en {carpeta_salida}")


num_fragmentos = int(sys.argv[1])
ruta_csv = sys.argv[2]

preprocessing_codvar(num_fragmentos, ruta_csv)