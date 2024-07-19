import pandas as pd
import cargar_datos as carga

def clean_date():
    # Cargar datos
    data = carga.get_datasets()
    df_viajes = data['viajes']
    df_paradas = data['paradas']
    df_origen_destino_linea = data['origen_destino_linea']
    df_orden_paradas = data['orden_paradas']

    # Limpieza de datos
    df_viajes_no_null = df_viajes.dropna(subset=['fecha_evento', 'cantidad_pasajeros', 'codigo_parada_origen', 'cantidad_pasajeros', 'dsc_linea', 'sevar_codigo'])
    df_paradas_no_null = df_paradas.dropna(subset=['COD_UBIC_P', 'DESC_LINEA', 'COD_VARIAN', 'X', 'Y'])
    df_origen_destino_linea_no_null = df_origen_destino_linea.dropna(subset=['COD_LINEA', 'DESC_LINEA', 'COD_VARIAN', 'DESC_VARIA'])
    df_orden_paradas_no_null = df_orden_paradas.dropna(subset=['tipo_dia', 'cod_variante', 'frecuencia', 'cod_ubic_parada', 'hora', 'dia_anterior'])

    # Filtrar las columnas necesarias
    df_viajes = df_viajes_no_null[['fecha_evento', 'cantidad_pasajeros', 'codigo_parada_origen', 'dsc_linea', 'sevar_codigo']]
    df_origen_destino_linea = df_origen_destino_linea_no_null[['COD_LINEA', 'DESC_LINEA', 'COD_VARIAN', 'DESC_VARIA']]
    df_paradas = df_paradas_no_null[['COD_UBIC_P', 'DESC_LINEA', 'COD_VARIAN', 'X', 'Y']]
    df_orden_paradas = df_orden_paradas_no_null[['tipo_dia', 'cod_variante', 'frecuencia', 'cod_ubic_parada', 'hora', 'dia_anterior']]

    # Realizar el merge para obtener las paradas de una línea específica considerando COD_VARIAN y DESC_VARIA
    df_paradas_lineas_direc = pd.merge(
        df_paradas,
        df_origen_destino_linea[['DESC_LINEA', 'COD_VARIAN', 'DESC_VARIA']],
        on=['DESC_LINEA', 'COD_VARIAN']
    )

    # Ordenar las paradas segun la linea
    df_dia_anterior_N = df_orden_paradas[df_orden_paradas['dia_anterior'] =='N']
    df_first_frequencies = df_dia_anterior_N.groupby('cod_variante').first().reset_index()
    df_orden_paradas = df_dia_anterior_N.merge(df_first_frequencies[['cod_variante', 'frecuencia']], on=['cod_variante', 'frecuencia'])

    # Realizar el segundo merge para agregar la información de los horarios
    df_paradas_lineas_direc = pd.merge(
        df_paradas_lineas_direc,
        df_orden_paradas,
        left_on=['COD_UBIC_P', 'COD_VARIAN'],
        right_on=['cod_ubic_parada', 'cod_variante']
    )

    # Ordenar el DataFrame resultante por línea, variante, y hora
    df_paradas_lineas_direc = df_paradas_lineas_direc.sort_values(by=['DESC_LINEA', 'frecuencia', 'hora'])
    return {
        'viajes': df_viajes, 
        #'orden_paradas': df_orden_paradas, 'origen_destino_linea': df_origen_destino_linea,
        'paradas_lineas_direc': df_paradas_lineas_direc[
            ['COD_UBIC_P', 'DESC_LINEA', 'COD_VARIAN', 'X', 'Y', 'DESC_VARIA']
        ]
    }

clean_date()