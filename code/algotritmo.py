import pandas as pd
import preprocessing_data as pre_data
from datetime import datetime, timedelta

def heuristica1():
    #Obtener los datos 
    data = pre_data.clean_date()
    df_viajes = data['viajes']
    df_paradas = data['paradas']
    df_paradas_lineas_direc = data['paradas_lineas_direc']

    # Lista de paradas por cordenadas para medir distancias
    df_paradas_lineas_direc = data['paradas_lineas_direc']
    df_paradas_x = df_paradas_lineas_direc[['COD_UBIC_P', 'X']].drop_duplicates()
    df_paradas_y = df_paradas_lineas_direc[['COD_UBIC_P', 'Y']].drop_duplicates()
    df_paradas_x_sorted = df_paradas_x.sort_values(by='X')
    df_paradas_y_sorted = df_paradas_y.sort_values(by='Y')
    
    # Convertir la columna 'fecha_evento' a datetime
    df_viajes['fecha_evento'] = pd.to_datetime(df_viajes['fecha_evento'])

    # Extraer la hora de 'fecha_evento'
    df_viajes['hora'] = df_viajes['fecha_evento'].dt.hour

    # Agrupar por 'codigo_parada_origen', 'dsc_linea', 'sevar_codigo' y 'hora', luego contar los viajes en cant_viajes
    df_cant_viajes_hora = df_viajes.groupby(['codigo_parada_origen', 'dsc_linea', 'sevar_codigo', 'hora'])['cantidad_pasajeros'].sum().reset_index()

    # Renombrar las columnas para que coincidan con el formato deseado
    df_cant_viajes_hora.columns = ['COD_UBIC_P', 'DESC_LINEA', 'COD_VARIAN', 'hora', 'cant_viajes']
    i = 0
    # Crear franjas horarias de una hora
    horas = [f"{str(h).zfill(2)}:00 - {str(h+1).zfill(2)}:00" for h in range(24)]

    # for cod_ubic_p in df_paradas['COD_UBIC_P']:
    #     df_lineas_para_x_parada = df_paradas[df_paradas['COD_UBIC_P'] == cod_ubic_p]
    #     for _, linea in df_lineas_para_x_parada.iterrows():
    #         for hora in horas:
    #         iter_de_calculo(
    #             cod_ubic_p, linea['DESC_LINEA'], linea['COD_VARIAN'],
    #             hora, df_paradas_x_sorted, df_paradas_y_sorted, 
    #             df_cant_viajes_hora, df_paradas_lineas_direc
    #         )
    iter_de_calculo(
                4211, '546', 217,
                8, df_paradas_x_sorted, df_paradas_y_sorted, 
                df_cant_viajes_hora, df_paradas_lineas_direc
            )

#     df_cant_viajes_hora
#           COD_UBIC_P DESC_LINEA  COD_VARIAN  hora  cant_viajes
# 93761        3217        100         242     5            2
# 93762        3217        100         242     6            6
# 93763        3217        100         242     7           18
# 93764        3217        100         242     8           26
# 93765        3217        100         242     9           14
    
    # prueba para medir la distancia
    # distanncia_paradas(556, df_paradas_x_sorted, df_paradas_y_sorted)


#funcion para quedarnos con lo necesario cada vez que se toma una row de viajes. Cod parada, línea y variante en la que la persona asciende
def iter_de_calculo(
        cod_parada: int, desc_linea: str, cod_var: int, fecha_iter: datetime, 
        df_paradas_x_sorted: pd.DataFrame, df_paradas_y_sorted: pd.DataFrame,
        df_cant_viajes_hora: pd.DataFrame, data_paradas_lineas_direc: pd.DataFrame
        ):
    # 1-descarto viajes que no me sirven (descarto franjas)
    # data = pre_data.clean_date()
    # data_viajes_iter = data['viajes']

    # Convertir 'fecha_evento' a formato datetime
    # data_viajes_iter['fecha_evento'] = pd.to_datetime(data_viajes_iter['fecha_evento'])

    # Definir el límite de tiempo (1 hora 40 minutos)
    # limite_tiempo = timedelta(hours=1, minutes=40)

    # Filtrar los registros
    # filtro = data_viajes_iter['fecha_evento'].apply(lambda x: x <= fecha_iter + limite_tiempo)
    # data_viajes_iter = data_viajes_iter[filtro]

    #2-busco paradas siguientes a la que sube
    # Filtrar los registros que coinciden con desc_linea y cod_var y están después de cod_parada
    
    # Aplica las dos condiciones a las filas posteriores
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc[
        (data_paradas_lineas_direc['COD_VARIAN'] == cod_var)
    ]

    # Resetea los índices y elimina la columna de índices original
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter.reset_index(drop=True)

    # print(data_paradas_lineas_direc_iter)

    # Encuentra el índice de la primera fila que cumple con las tres condiciones
    index_initial = data_paradas_lineas_direc_iter[
        (data_paradas_lineas_direc_iter['COD_UBIC_P'] == cod_parada)
    ].index[0]

    print(index_initial)
    # Filtra el DataFrame desde la fila que cumple con las tres condiciones
    filtered_data = data_paradas_lineas_direc_iter.iloc[index_initial + 1:]

    print(filtered_data)

    # print(data_paradas_lineas_direc_iter)
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter.drop_duplicates()
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter[['COD_UBIC_P', 'X', 'Y']] #Me quedo solo con los cod ubicacion parada y las coordenadas.


    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter.assign(VOLUMEN=0)
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter.assign(PROBABILIDAD=0)
    # #3-para cada una de esas paradas, busco las mas cercanas y lineas que tengan como retorno una parada cercana a la que sube y calculo volumen.
    # # Definir la distancia máxima en metros
    # distancia_maxima = 250


    # # Lista para almacenar los resultados
    # paradas_cercanas = []


    # data_paradas_iter = data['paradas']
    # # Iterar sobre cada fila en data_paradas_lineas_direc_iter
    # for index, row in data_paradas_lineas_direc_iter.iterrows():
    #     cod_ubic_p = row['COD_UBIC_P']
    #     x1, y1 = row['X'], row['Y']
       
    #     # Filtrar paradas cercanas en data_paradas_iter
    #     for _, parada in data_paradas_iter.iterrows():
    #         x2, y2 = parada['X'], parada['Y']
           
    #         # Calcular la distancia euclidiana
    #         distancia = np.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)
           
    #         # Comprobar si la distancia es menor que la distancia máxima y que no sea la misma parada
    #         if distancia < distancia_maxima and parada['COD_UBIC_P'] != cod_ubic_p:
    #             paradas_cercanas.append(parada)
               
    #     # Convertir la lista de paradas cercanas en un DataFrame
    #     df_paradas_cercanas = pd.DataFrame(paradas_cercanas)
    #     #busco lienas en sentido inverso (desc_var) al "princial" que tengan las paradas cercanas y la  parada origen en el recorrido y con esto calculamos volumen.
    #     df_lineas_cercanas_filtrado = df_paradas_cercanas[['DESC_LINEA']].drop_duplicates()
    #     df_paradas_lineas_direc = data['paradas_lineas_direc']


    #     # Lista para almacenar los resultados
    #     lineas_paradas_cercanas = []


    #     # Iterar sobre cada línea en df_lineas_cercanas_filtrado
    #     for index, linea in df_lineas_cercanas_filtrado.iterrows():
    #         desc_linea = linea[['DESC_LINEA', 'COD_UBIC_P']]
           
    #         # Condición 1: DESC_LINEA = desc_linea, COD_UBIC_P = cod_parada, DESC_VARIA != desc_var
    #         condicion1 = df_paradas_lineas_direc[
    #             (df_paradas_lineas_direc['DESC_LINEA'] == desc_linea) &
    #             (df_paradas_lineas_direc['COD_UBIC_P'] == cod_parada) &
    #             (df_paradas_lineas_direc['DESC_VARIA'] != desc_var)
    #         ]
           
    #         # Condición 2: DESC_LINEA = desc_linea, COD_UBIC_P = COD_UBIC_P de df_paradas_cercanas, DESC_VARIA != desc_var
    #         condicion2 = df_paradas_lineas_direc[
    #             (df_paradas_lineas_direc['DESC_LINEA'] == desc_linea) &
    #             (df_paradas_lineas_direc['COD_UBIC_P'] == cod_ubic_p) &
    #             (df_paradas_lineas_direc['DESC_VARIA'] != desc_var)
    #         ]
           
    #         # Verificar si ambas condiciones existen
    #         if not condicion1.empty and not condicion2.empty:
    #              lineas_paradas_cercanas.append(desc_linea)


    #         #Ahora con esta lineas, busco el volumen de viajes y acumulo en una variable global total viajes regreso, que va a ser el dividendo de los volumenes por parada para hacer el calculo de prob= casos fav / casos posibles.
    #         #TENGO Q ITERAR SOBRE PARADAS CERCANAS Y ACUMULAR LOS VIAJES = VOLUMEN.
    #         acc_volumen = 0
    #         # Iterar sobre cada fila en lineas_paradas_cercanas
    #         for index, linea_parada_cercana in lineas_paradas_cercanas.iterrows():
    #             desc_linea = linea_parada_cercana['DESC_LINEA']
    #             cod_ubic_p = linea_parada_cercana['COD_UBIC_P']
               
    #             # Filtrar data_viajes_iter por COD_UBIC_P y DESC_LINEA
    #             viajes_filtrados = data_viajes_iter[
    #                 (data_viajes_iter['codigo_parada_origen'] == cod_ubic_p) &
    #                 (data_viajes_iter['dsc_linea'] == desc_linea)
    #             ]
               
    #             # Acumular el número de viajes en acc_volumen
    #             acc_volumen += len(viajes_filtrados)
       
    #     # Crear una máscara para seleccionar la fila específica
    #     mascara = (
    #         (data_paradas_lineas_direc_iter['COD_UBIC_P'] == cod_ubic_p) &
    #         (data_paradas_lineas_direc_iter['X'] == x1) &
    #         (data_paradas_lineas_direc_iter['Y'] == y1)
    #     )


    #     # Asignar el nuevo valor a la columna VOLUMEN para las filas que coincidan con la máscara
    #     data_paradas_lineas_direc_iter.loc[mascara, 'VOLUMEN'] = acc_volumen                  


    # volumen_total = data_paradas_lineas_direc_iter['VOLUMEN'].sum()
    # for index, row in data_paradas_lineas_direc_iter.iterrows():
    #     data_paradas_lineas_direc_iter.at[index, 'PROBABILIDAD'] = row['VOLUMEN'] / volumen_total
    # return data_viajes_iter

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


heuristica1()

    