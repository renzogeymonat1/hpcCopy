import pandas as pd
import preprocessing_data as pre_data
import numpy as np
import json
import sys
import socket

paradas_cercanas = {}

def main(numFragmento):
    connected = False

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(('localhost', 6500 + numFragmento))
        connected = True
        print("Conectado al servidor.")
    except ConnectionRefusedError:
        print(f"Conexión rechazada, intento.")
        
    if not connected:
        print("No se pudo conectar al servidor.")
        exit()

    # Recibir datos JSON desde el servidor
    received_data = b""
    end_flag = b"END_OF_DATA"
    while True:
        data = client_socket.recv(1024)
        if end_flag in data:
            received_data += data[:data.find(end_flag)]
            break
        received_data += data

    try:
        json_data = json.loads(received_data.decode())
        print("Datos JSON recibidos:")
        #print(json.dumps(json_data, indent=4))
    except json.JSONDecodeError as e:
        print(f"Error al decodificar JSON: {e}")

    data_paradas_lineas_direc = pd.DataFrame.from_records(json_data['paradas_lineas_direc'])
    data_cod_varian = pd.DataFrame.from_records(json_data['cod_varian'])
    df_cant_viajes_franja = pd.DataFrame.from_records(json_data['df_cant_viajes_franja'])

    # Lista de paradas por cordenadas para medir distancias
    #data_paradas_lineas_direc = data['paradas_lineas_direc']
    df_paradas_x = data_paradas_lineas_direc[['COD_UBIC_P', 'X']].drop_duplicates()
    df_paradas_y = data_paradas_lineas_direc[['COD_UBIC_P', 'Y']].drop_duplicates()
    df_paradas_x_sorted = df_paradas_x.sort_values(by='X')
    df_paradas_y_sorted = df_paradas_y.sort_values(by='Y')
    
    # Crear un diccionario vacío
    resultados = {}
    k = 0
    for franja in ['00-10','10-18','18-00']:
        for _, row in data_cod_varian.iterrows():
            k += 1
            if k <= 3:
                data_paradas_variante = data_paradas_lineas_direc[data_paradas_lineas_direc['COD_VARIAN'] == row['COD_VARIAN']]
                data_paradas_variante =  data_paradas_variante[['COD_UBIC_P']]
                data_paradas_variante =  data_paradas_variante[['COD_UBIC_P']].drop_duplicates()
                        
                # Obtener los valores de la columna 'COD_UBIC_P'
                cod_ubic_p_values = data_paradas_variante['COD_UBIC_P'].values

                # Crear una matriz nxn, donde n es el número de filas en data_paradas_variante
                n = len(cod_ubic_p_values)
                matriz = np.zeros((n, n + 1))

                # Asignar los valores de cod_ubic_p_values a la primera columna de la matriz
                matriz[:, 0] = cod_ubic_p_values

                i = 1
                for cod_ubic_p_acenso in data_paradas_variante['COD_UBIC_P']:
                    data_probabilidades_iter = iter_de_calculo(
                            cod_ubic_p_acenso, row['DESC_LINEA'], row['COD_VARIAN'],
                            franja, df_paradas_x_sorted, df_paradas_y_sorted, 
                            df_cant_viajes_franja, data_paradas_lineas_direc
                        )
                    print(cod_ubic_p_acenso)
                    prob_values = data_probabilidades_iter['PROBABILIDAD'].values
                    prob_values = np.pad(prob_values, (n - len(prob_values), 0), 'constant')
                    matriz[:, i] = prob_values
                    i += 1
                np.set_printoptions(suppress=True, precision=8)
                key = (row['COD_VARIAN'], franja)
                resultados[key] = matriz

    # Convertir las matrices de NumPy a listas para almacenarlas en JSON
    resultado = {str(key): matriz.tolist() for key, matriz in resultados.items()}

   # Convertir el diccionario a una cadena JSON
    json_data = json.dumps(resultado)

    # Enviar el JSON
    client_socket.sendall(json_data.encode())
    client_socket.close()

#funcion para quedarnos con lo necesario cada vez que se toma una row de viajes. Cod parada, línea y variante en la que la persona asciende
def iter_de_calculo(
        cod_parada: int, desc_linea: str, cod_var: int, franja_iter: str, 
        df_paradas_x_sorted: pd.DataFrame, df_paradas_y_sorted: pd.DataFrame,
        df_cant_viajes_franja: pd.DataFrame, data_paradas_lineas_direc: pd.DataFrame
        ):
    
    # Filtrar los registros que coinciden con desc_linea y cod_var y están después de cod_parada
    # Aplica las dos condiciones a las filas posteriores
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc[
        (data_paradas_lineas_direc['COD_VARIAN'] == cod_var)
    ]

    # Resetea los índices y elimina la columna de índices original
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter.reset_index(drop=True)

    # Encuentra el índice de la primera fila que cumple con la condicion
    index_initial = data_paradas_lineas_direc_iter[
            (data_paradas_lineas_direc_iter['COD_UBIC_P'] == cod_parada)
        ].index[0]

    # Filtra el DataFrame desde la fila que cumple con las dos condiciones (me quedo con las siguientes paradas a la que sube por eso el +1)
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter.iloc[index_initial + 1:]

    #Sentido/direccion del viaje antes de quitar columnas
    if not data_paradas_lineas_direc_iter.empty:
        sentidoViaje = data_paradas_lineas_direc_iter.iloc[0]['DESC_VARIA']

    # print(data_paradas_lineas_direc_iter)
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter.drop_duplicates(subset=['COD_UBIC_P'])

    #Agrego columnas VOLUMEN y PROBABILIDAD para luego hacer los calculos.
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter.assign(VOLUMEN=0)
    data_paradas_lineas_direc_iter = data_paradas_lineas_direc_iter.assign(PROBABILIDAD=0)
    if cod_parada not in paradas_cercanas:
        #2-para cada una de esas paradas, busco las mas cercanas y lineas que tengan como retorno una parada cercana a la que sube y calculo volumen.
        data_paradas_cercanas_a_origen = distanncia_paradas(cod_parada, df_paradas_x_sorted, df_paradas_y_sorted)
        paradas_cercanas[cod_parada] = data_paradas_cercanas_a_origen
        origen_paradas_cercanas_list = data_paradas_cercanas_a_origen['COD_UBIC_P'].tolist()
    else:
        origen_paradas_cercanas_list = paradas_cercanas[cod_parada]['COD_UBIC_P'].tolist()

    # Filtrar el tercer DataFrame para obtener las líneas que pasan por las paradas de origen
    data_lineas_origen = data_paradas_lineas_direc[data_paradas_lineas_direc['COD_UBIC_P'].isin(origen_paradas_cercanas_list)]
    #Cambio de nombre para columna para el dataframe data_lineas_origen le agrego a '_CERCANA_ORIGEN' a las columnas
    data_lineas_origen = data_lineas_origen.rename(columns=lambda x: f'{x}_CERCANA_ORIGEN')

    # Iterar sobre cada fila en data_paradas_lineas_direc_iter
    for _, row in data_paradas_lineas_direc_iter.iterrows(): 
        #Busco lineas que que pasen entre las cercanas del origen y las cercanas de las paradas siguientes/proximas.
        cod_parada_siguiente = row['COD_UBIC_P']
        if cod_parada_siguiente not in paradas_cercanas:
            data_paradas_cercanas_a_siguientes = distanncia_paradas(cod_parada_siguiente, df_paradas_x_sorted, df_paradas_y_sorted)
            # Obtener listas de COD_UBIC_P de las paradas cercanas.
            paradas_cercanas[cod_parada_siguiente] = data_paradas_cercanas_a_siguientes
            siguientes_paradas_cercanas_list = data_paradas_cercanas_a_siguientes['COD_UBIC_P'].tolist()
        else:
            siguientes_paradas_cercanas_list = paradas_cercanas[cod_parada_siguiente]['COD_UBIC_P'].tolist()

        # Obtener las líneas que pasan por las paradas cercanas a las siguientes
        data_lineas_siguientes = data_paradas_lineas_direc[data_paradas_lineas_direc['COD_UBIC_P'].isin(siguientes_paradas_cercanas_list)]
  
        # Unir los DataFrames data_lineas_origen y data_lineas_siguientes por DESC_LINEA_CERCANA_ORIGEN = DESC_LINEA y COD_VARIAN_CERCANA_ORIGEN = COD_VARIAN
        data_lineas_regreso = pd.merge(data_lineas_origen, data_lineas_siguientes,
                                    left_on=['DESC_LINEA_CERCANA_ORIGEN', 'COD_VARIAN_CERCANA_ORIGEN'],
                                    right_on=['DESC_LINEA', 'COD_VARIAN'],
                                    how='inner')

        # Seleccionar las columnas específicas
        data_lineas_regreso = data_lineas_regreso.loc[:, 
            [
                'COD_UBIC_P_CERCANA_ORIGEN', 'DESC_LINEA_CERCANA_ORIGEN',
                'COD_VARIAN_CERCANA_ORIGEN', 'DESC_VARIA_CERCANA_ORIGEN', 
                'COD_UBIC_P', 'DESC_LINEA', 'COD_VARIAN', 'DESC_VARIA'
            ]]
        
        # Filtrar los registros donde DESC_VARIA sea distinto de sentidoViaje
        data_lineas_regreso = data_lineas_regreso[data_lineas_regreso['DESC_VARIA'] != sentidoViaje]
 
        # Seleccionar solo las columnas deseadas y Eliminar duplicados del DataFrame filtrado
        data_lineas_regreso = data_lineas_regreso[['COD_UBIC_P', 'DESC_LINEA', 'COD_VARIAN', 'DESC_VARIA']].drop_duplicates()

        #Ahora que tengo todas las lineas y las paradas cercanas para posibles regresos, cuento viajes para calcular volumen. 
        acc_volumen = 0
        for _, row in data_lineas_regreso.iterrows():
            cod_ubic_p = row['COD_UBIC_P']
            #print(data_paradas_cercanas_a_origen)
            if cod_ubic_p not in origen_paradas_cercanas_list:
                desc_linea = row['DESC_LINEA']
                cod_varian = row['COD_VARIAN']
                if franja_iter == '00-10':
                    # Cálculo específico para la franja '00-10'
                    row_cant_viajes = df_cant_viajes_franja[
                        (df_cant_viajes_franja['COD_UBIC_P'] == cod_ubic_p) &
                        (df_cant_viajes_franja['DESC_LINEA'] == desc_linea) &
                        (df_cant_viajes_franja['COD_VARIAN'] == cod_varian) &
                        (df_cant_viajes_franja['franja_horaria'] == '10-18')
                    ]
                    acc_volumen += row_cant_viajes['cant_viajes'].sum()
                    
                    row_cant_viajes2 = df_cant_viajes_franja[
                        (df_cant_viajes_franja['COD_UBIC_P'] == cod_ubic_p) &
                        (df_cant_viajes_franja['DESC_LINEA'] == desc_linea) &
                        (df_cant_viajes_franja['COD_VARIAN'] == cod_varian) &
                        (df_cant_viajes_franja['franja_horaria'] == '18-00')
                    ]
                    acc_volumen += row_cant_viajes2['cant_viajes'].sum()

                elif franja_iter == '10-18':
                    # Cálculo específico para la franja '10-18'
                    row_cant_viajes = df_cant_viajes_franja[
                        (df_cant_viajes_franja['COD_UBIC_P'] == cod_ubic_p) &
                        (df_cant_viajes_franja['DESC_LINEA'] == desc_linea) &
                        (df_cant_viajes_franja['COD_VARIAN'] == cod_varian) 
                    ]
                    acc_volumen += row_cant_viajes['cant_viajes'].sum()

                elif franja_iter == '18-00':
                    # Cálculo específico para la franja '18-00'
                    row_cant_viajes = df_cant_viajes_franja[
                        (df_cant_viajes_franja['COD_UBIC_P'] == cod_ubic_p) &
                        (df_cant_viajes_franja['DESC_LINEA'] == desc_linea) &
                        (df_cant_viajes_franja['COD_VARIAN'] == cod_varian) &
                        (df_cant_viajes_franja['franja_horaria'] == '00-10')
                    ]
                    acc_volumen += row_cant_viajes['cant_viajes'].sum()

                    row_cant_viajes2 = df_cant_viajes_franja[
                        (df_cant_viajes_franja['COD_UBIC_P'] == cod_ubic_p) &
                        (df_cant_viajes_franja['DESC_LINEA'] == desc_linea) &
                        (df_cant_viajes_franja['COD_VARIAN'] == cod_varian) &
                        (df_cant_viajes_franja['franja_horaria'] == '10-18')
                    ]
                    acc_volumen += row_cant_viajes2['cant_viajes'].sum()
        # Actualizar el valor de la columna 'VOLUMEN' para la fila que cumple la condición
        data_paradas_lineas_direc_iter.loc[
        data_paradas_lineas_direc_iter['COD_UBIC_P'] == cod_parada_siguiente, 'VOLUMEN'
        ] = acc_volumen
    volumen_total = 0
    volumen_total = data_paradas_lineas_direc_iter['VOLUMEN'].sum()

    #Calculo de probabilidad para todas las paradas siguientes.
    if volumen_total != 0:
        data_paradas_lineas_direc_iter['PROBABILIDAD'] = (data_paradas_lineas_direc_iter['VOLUMEN'] / volumen_total) * 100
    else: 
        data_paradas_lineas_direc_iter['PROBABILIDAD'] = 0
    return data_paradas_lineas_direc_iter


#calcular la distancia entre paradas
def distanncia_paradas(cod_parada: int, df_x: pd.DataFrame, df_y: pd.DataFrame):
    # Calcular las paradas mas cercanas a cod_parada
    max_distance = 150
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
        if abs(row['X'] - parada_referencia_x) < max_distance and row['COD_UBIC_P'] != cod_parada:
            df_paradas_x = pd.concat([df_paradas_x, pd.DataFrame([row[['COD_UBIC_P', 'X']]])], ignore_index=True)
    
    for _, row in df_y.iterrows():
        if abs(row['Y'] - parada_referencia_y) < max_distance:
           df_paradas_y = pd.concat([df_paradas_y, pd.DataFrame([row[['COD_UBIC_P', 'Y']]])], ignore_index=True)
  
    # Combinar los DataFrames resultantes en uno solo
    df_resultado = pd.merge(df_paradas_x, df_paradas_y, on=['COD_UBIC_P'])

    # Convertir la columna COD_UBIC_P a enteros
    df_resultado['COD_UBIC_P'] = df_resultado['COD_UBIC_P'].astype(int)

    return df_resultado

if __name__ == "__main__":
    num_fragmento = int(sys.argv[1])
    result = main(num_fragmento)
    #print(json.dumps(result)) 
