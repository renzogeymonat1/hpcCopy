import pandas as pd
import preprocessing_data as pre_data
from datetime import datetime, timedelta
import numpy as np

def heuristica1():
    #Obtener los datos 
    data = pre_data.clean_date()
    df_viajes = data['viajes']
    df_paradas = data['paradas']
    data_paradas_lineas_direc = data['paradas_lineas_direc']

    # Lista de paradas por cordenadas para medir distancias
    data_paradas_lineas_direc = data['paradas_lineas_direc']
    df_paradas_x = data_paradas_lineas_direc[['COD_UBIC_P', 'X']].drop_duplicates()
    df_paradas_y = data_paradas_lineas_direc[['COD_UBIC_P', 'Y']].drop_duplicates()
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

    # # Seleccionar solo la columna COD_VARIAN
    # data_cod_varian = data_paradas_lineas_direc[['COD_VARIAN']]
    
    # #Eliminar duplicados
    # data_cod_varian =  data_cod_varian[['COD_VARIAN']].drop_duplicates()
    # print(data_cod_varian)

    # # Printeo de prueba para ver las paradas del recorrido.
    # data_paradas_variante = data_paradas_lineas_direc[data_paradas_lineas_direc['COD_VARIAN'] == 217]
    # data_paradas_variante =  data_paradas_variante[['COD_UBIC_P']]
    # data_paradas_variante =  data_paradas_variante[['COD_UBIC_P']].drop_duplicates()
    # # print(data_paradas_variante)
    # # Obtener los valores de la columna 'COD_UBIC_P'
    # cod_ubic_p_values = data_paradas_variante['COD_UBIC_P'].values

    # # Crear una matriz nxn, donde n es el número de filas en data_paradas_variante
    # n = len(cod_ubic_p_values)
    # matriz = np.zeros((n, n + 1))

    # # Asignar los valores de cod_ubic_p_values a la primera columna de la matriz
    # matriz[:, 0] = cod_ubic_p_values

    # # Imprimir la matriz para verificar
    # print(matriz)

    # i = 1
    # for cod_ubic_p_acenso in data_paradas_variante['COD_UBIC_P']:
    #     data_probabilidades_iter = iter_de_calculo(
    #             cod_ubic_p_acenso, '546', 217,
    #             8, df_paradas_x_sorted, df_paradas_y_sorted, 
    #             df_cant_viajes_hora, data_paradas_lineas_direc
    #         )
    #     prob_values = data_probabilidades_iter['PROBABILIDAD'].values
    #     prob_values = np.pad(prob_values, (n - len(prob_values), 0), 'constant')
    #     #print(prob_values)
    #     matriz[:, i] = prob_values
    #     i += 1
    # np.set_printoptions(suppress=True, precision=8)
    # print(matriz)

    # Crear un diccionario vacío
    dict = {}

    # for hora in horas:
    #     for cod_variante in data_cod_varian['COD_VARIAN']:
    data_paradas_variante = data_paradas_lineas_direc[data_paradas_lineas_direc['COD_VARIAN'] == 217]
    data_paradas_variante =  data_paradas_variante[['COD_UBIC_P']]
    data_paradas_variante =  data_paradas_variante[['COD_UBIC_P']].drop_duplicates()
            
    # Obtener los valores de la columna 'COD_UBIC_P'
    cod_ubic_p_values = data_paradas_variante['COD_UBIC_P'].values

    # Crear una matriz nxn, donde n es el número de filas en data_paradas_variante
    n = len(cod_ubic_p_values)
    matriz = np.zeros((n, n + 1))

    # Asignar los valores de cod_ubic_p_values a la primera columna de la matriz
    matriz[:, 0] = cod_ubic_p_values

    # Imprimir la matriz para verificar
    print(matriz)

    i = 1
    for cod_ubic_p_acenso in data_paradas_variante['COD_UBIC_P']:
        data_probabilidades_iter = iter_de_calculo(
                cod_ubic_p_acenso, '546', 217,
                8, df_paradas_x_sorted, df_paradas_y_sorted, 
                df_cant_viajes_hora, data_paradas_lineas_direc
            )
        prob_values = data_probabilidades_iter['PROBABILIDAD'].values
        prob_values = np.pad(prob_values, (n - len(prob_values), 0), 'constant')
        #print(prob_values)
        matriz[:, i] = prob_values
        i += 1
    np.set_printoptions(suppress=True, precision=8)
    print(matriz)
    key = (217, 8)
    dict[key] = matriz

    # iter_de_calculo(
    #             3692, '546', 217,
    #             8, df_paradas_x_sorted, df_paradas_y_sorted, 
    #             df_cant_viajes_hora, data_paradas_lineas_direc
    #         )

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
    
    #1-busco paradas siguientes a la que sube
    # Filtrar los registros que coinciden con desc_linea y cod_var y están después de cod_parada
    # Aplica las dos condiciones a las filas posteriores
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc[
        (data_paradas_lineas_direc['COD_VARIAN'] == cod_var)
    ]

    # Resetea los índices y elimina la columna de índices original
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter.reset_index(drop=True)

    # print(data_paradas_lineas_direc_iter)

    # Encuentra el índice de la primera fila que cumple con las dos condiciones
    index_initial = data_paradas_lineas_direc_iter[
            (data_paradas_lineas_direc_iter['COD_UBIC_P'] == cod_parada)
        ].index[0]

    print(index_initial)
    # Filtra el DataFrame desde la fila que cumple con las dos condiciones (me quedo con las siguientes paradas a la que sube por eso el +1)
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter.iloc[index_initial + 1:]

    #Sentido/direccion del viaje antes de quitar columnas
    if not data_paradas_lineas_direc_iter.empty:
        sentidoViaje = data_paradas_lineas_direc_iter.iloc[0]['DESC_VARIA']
        print(sentidoViaje)

    print(data_paradas_lineas_direc_iter)
    # print(data_paradas_lineas_direc_iter)
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter.drop_duplicates()

    #Agrego columnas VOLUMEN y PROBABILIDAD para luego hacer los calculos.
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter.assign(VOLUMEN=0)
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter.assign(PROBABILIDAD=0)

    print(data_paradas_lineas_direc_iter)
    
    #2-para cada una de esas paradas, busco las mas cercanas y lineas que tengan como retorno una parada cercana a la que sube y calculo volumen.
    data_paradas_cercanas_a_origen = distanncia_paradas(cod_parada, df_paradas_x_sorted, df_paradas_y_sorted)
    print(data_paradas_cercanas_a_origen)

    origen_paradas_cercanas_list = data_paradas_cercanas_a_origen['COD_UBIC_P'].tolist()
    print(origen_paradas_cercanas_list)

    # Filtrar el tercer DataFrame para obtener las líneas que pasan por las paradas de origen
    data_lineas_origen = data_paradas_lineas_direc[data_paradas_lineas_direc['COD_UBIC_P'].isin(origen_paradas_cercanas_list)]
    #Cambio de nombre para columna para el dataframe data_lineas_origen le agrego a '_CERCANA_ORIGEN' a las columnas
    data_lineas_origen = data_lineas_origen.rename(columns=lambda x: f'{x}_CERCANA_ORIGEN')
    print(data_lineas_origen)

    # Iterar sobre cada fila en data_paradas_lineas_direc_iter
    for index, row in data_paradas_lineas_direc_iter.iterrows(): 
        #Busco lineas que que pasen entre las cercanas del origen y las cercanas de las paradas siguientes/proximas.
        cod_parada_siguiente = row['COD_UBIC_P']
        data_paradas_cercanas_a_siguientes = distanncia_paradas(cod_parada_siguiente, df_paradas_x_sorted, df_paradas_y_sorted)
        #print(data_paradas_cercanas_a_siguientes)
        
        # Obtener listas de COD_UBIC_P de las paradas cercanas.
        siguientes_paradas_cercanas_list = data_paradas_cercanas_a_siguientes['COD_UBIC_P'].tolist()

        # obteneR las líneas que pasan por las paradas cercanas a las siguientes
        data_lineas_siguientes = data_paradas_lineas_direc[data_paradas_lineas_direc['COD_UBIC_P'].isin(siguientes_paradas_cercanas_list)]
        print( data_lineas_siguientes)

        # Unir los DataFrames data_lineas_origen y data_lineas_siguientes por DESC_LINEA_CERCANA_ORIGEN = DESC_LINEA y COD_VARIAN_CERCANA_ORIGEN = COD_VARIAN
        data_lineas_regreso = pd.merge(data_lineas_origen, data_lineas_siguientes,
                                    left_on=['DESC_LINEA_CERCANA_ORIGEN', 'COD_VARIAN_CERCANA_ORIGEN'],
                                    right_on=['DESC_LINEA', 'COD_VARIAN'],
                                    how='inner')

        # Seleccionar las columnas específicas
        data_lineas_regreso = data_lineas_regreso.loc[:, ['COD_UBIC_P_CERCANA_ORIGEN', 'DESC_LINEA_CERCANA_ORIGEN',
                                                  'COD_VARIAN_CERCANA_ORIGEN', 'DESC_VARIA_CERCANA_ORIGEN', 'COD_UBIC_P', 'DESC_LINEA', 'COD_VARIAN', 'DESC_VARIA']]

        # Filtrar los registros donde DESC_VARIA sea distinto de sentidoViaje
        data_lineas_regreso = data_lineas_regreso[data_lineas_regreso['DESC_VARIA'] != sentidoViaje]

        # Eliminar duplicados del DataFrame filtrado
        data_lineas_regreso = data_lineas_regreso.drop_duplicates()

        # Seleccionar solo las columnas deseadas
        data_lineas_regreso = data_lineas_regreso[['COD_UBIC_P', 'DESC_LINEA', 'COD_VARIAN', 'DESC_VARIA']]

        # Mostrar el DataFrame resultante
        print(data_lineas_regreso)

        print(df_cant_viajes_hora)
        #Ahora que tengo todas las lineas y las paradas cercanas para posibles regresos, cuento viajes para calcular volumen. 
        acc_volumen = 0
        for index, row in data_lineas_regreso.iterrows():
            cod_ubic_p = row['COD_UBIC_P']
            desc_linea = row['DESC_LINEA']
            cod_varian = row['COD_VARIAN']
            row_cant_viajes = df_cant_viajes_hora[
                (df_cant_viajes_hora['COD_UBIC_P'] == cod_ubic_p) &
                (df_cant_viajes_hora['DESC_LINEA'] == desc_linea) &
                (df_cant_viajes_hora['COD_VARIAN'] == cod_varian) &
                (df_cant_viajes_hora['hora'] == fecha_iter)
            ]

            acc_volumen += row_cant_viajes['cant_viajes'].sum()
        print(acc_volumen)
        # Actualizar el valor de la columna 'VOLUMEN' para la fila que cumple la condición
        data_paradas_lineas_direc_iter.loc[
        data_paradas_lineas_direc_iter['COD_UBIC_P'] == cod_parada_siguiente, 'VOLUMEN'
        ] = acc_volumen
    
    volumen_total = data_paradas_lineas_direc_iter['VOLUMEN'].sum()
    print(volumen_total)
    #Calculo de probabilidad para todas las paradas siguientes.
    data_paradas_lineas_direc_iter['PROBABILIDAD'] = (data_paradas_lineas_direc_iter['VOLUMEN'] / volumen_total) * 100
    print(data_paradas_lineas_direc_iter)
    return data_paradas_lineas_direc_iter

#calcular la distancia entre paradas
def distanncia_paradas(cod_parada: int, df_x: pd.DataFrame, df_y: pd.DataFrame):
    # Calcular las paradas mas cercanas a cod_parada
    max_distance = 100
    reference_stop_x = df_x[df_x['COD_UBIC_P'] == cod_parada]
    reference_stop_y = df_y[df_y['COD_UBIC_P'] == cod_parada]

    if reference_stop_x.empty:
        #print(f"No se encontró la parada con COD_UBIC_P igual a {cod_parada}")
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
            #print(df_paradas_x)
    
    for _, row in df_y.iterrows():
        if abs(row['Y'] - parada_referencia_y) < max_distance:
           df_paradas_y = pd.concat([df_paradas_y, pd.DataFrame([row[['COD_UBIC_P', 'Y']]])], ignore_index=True)
  
    # Combinar los DataFrames resultantes en uno solo
    df_resultado = pd.merge(df_paradas_x, df_paradas_y, on=['COD_UBIC_P'])

    # Convertir la columna COD_UBIC_P a enteros
    df_resultado['COD_UBIC_P'] = df_resultado['COD_UBIC_P'].astype(int)

    return df_resultado

heuristica1()

    