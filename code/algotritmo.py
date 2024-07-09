import pandas as pd
import preprocessing_data as pre_data
from datetime import datetime, timedelta

def heuristica1():
    #Obtener los datos 
    data = pre_data.clean_date()
    df_viajes = data['viajes']
    print(df_viajes)
    df_paradas_lineas_direc = data['paradas_lineas_direc']
    print(df_paradas_lineas_direc)

    # Lista de paradas por cordenadas para medir distancias
    df_paradas_lineas_direc = data['paradas_lineas_direc']
    df_paradas_x = df_paradas_lineas_direc[['COD_UBIC_P', 'X']].drop_duplicates()
    df_paradas_y = df_paradas_lineas_direc[['COD_UBIC_P', 'Y']].drop_duplicates()
    df_paradas_x_sorted = df_paradas_x.sort_values(by='X')
    df_paradas_y_sorted = df_paradas_y.sort_values(by='Y')
    
    # prueba para medir la distancia
    # distanncia_paradas(556, df_paradas_x_sorted, df_paradas_y_sorted)

    

#calcular la distancia entre paradas
def distanncia_paradas(cod_parada: int, df_x: pd.DataFrame, df_y: pd.DataFrame):
    # Calcular las paradas mas cercanas a cod_parada
    max_distance = 200
    reference_stop_x = df_x[df_x['COD_UBIC_P'] == cod_parada]
    reference_stop_y = df_y[df_y['COD_UBIC_P'] == cod_parada]

    if reference_stop_x.empty:
        print(f"No se encontró la parada con COD_UBIC_P igual a {cod_parada}")
        return
    parada_referencia_x = reference_stop_x.iloc[0]['X']
    parada_referencia_y = reference_stop_y.iloc[0]['Y']

    df_paradas_x = pd.DataFrame({
        'COD_UBIC_P':[], 'X':[]
    })
    df_paradas_y = pd.DataFrame({
        'COD_UBIC_P':[], 'Y':[]
    })

    for _, row in df_x.iterrows():
        if abs(row['X'] - parada_referencia_x) < max_distance:
            df_paradas_x = pd.concat([df_paradas_x, pd.DataFrame([row[['COD_UBIC_P', 'X']]])], ignore_index=True)
            print(df_paradas_x)
    
    for _, row in df_y.iterrows():
        if abs(row['Y'] - parada_referencia_y) < max_distance:
           df_paradas_y = pd.concat([df_paradas_y, pd.DataFrame([row[['COD_UBIC_P', 'Y']]])], ignore_index=True)

    return pd.merge(df_paradas_x, df_paradas_y, on=['COD_UBIC_P'])

#funcion para quedarnos con lo necesario cada vez que se toma una row de viajes. Cod parada, línea y variante en la que la persona asciende
def iter_de_calculo(cod_parada: int, desc_linea: str, cod_var: str, fecha_iter: datetime):
    #1-descarto viajes que no me sirven (descarto franjas)
    data = pre_data.clean_date()
    data_viajes_iter = data['viajes']

    # Convertir 'fecha_evento' a formato datetime
    data_viajes_iter['fecha_evento'] = pd.to_datetime(data_viajes_iter['fecha_evento'])

    # Definir el límite de tiempo (1 hora 40 minutos)
    limite_tiempo = timedelta(hours=1, minutes=40)

    # Filtrar los registros
    filtro = data_viajes_iter['fecha_evento'].apply(lambda x: x <= fecha_iter + limite_tiempo)
    data_viajes_iter = data_viajes_iter[filtro]

    #2-busco paradas siguientes a la que sube
    # Filtrar los registros que coinciden con desc_linea y cod_var y están después de cod_parada
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter[
        (data_paradas_lineas_direc_iter['COD_UBIC_P'] >= cod_parada) &  # Incluye cod_parada y los siguientes
        (data_paradas_lineas_direc_iter['DESC_LINEA'] == desc_linea) &
        (data_paradas_lineas_direc_iter['DESC_VARIA'] == cod_var)
    ]

    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter.drop_duplicates()

    #3-para cada una de esas paradas, busco las mas cercanas y lineas que tengan como retorno una parada cercana a la que sube y calculo volumen. 
    return data_viajes_iter
heuristica1()

    