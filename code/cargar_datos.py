import pandas as pd

# ubicacion de archivos
VIAJES = './csv/ROkzNjh5SCO63dSgeX8tcw.csv'
PARADAS = './csv/v_uptu_paradas.csv'
ORIGEN_DESTINO_LINEAS = './csv/v_uptu_lsv.csv'
ORDEN_PARADAS = './csv/uptu_pasada_variante.csv'

def get_datasets():
    # Leer el archivo CSV y cargar los primeros un millón de registros
    df_viajes = pd.read_csv(VIAJES)
    df_paradas = pd.read_csv(PARADAS, delimiter=';')
    df_origen_destino_linea = pd.read_csv(ORIGEN_DESTINO_LINEAS, delimiter=';')
    df_orden_paradas = pd.read_csv(ORDEN_PARADAS, delimiter=';')

    return { 
        'viajes': df_viajes, 'paradas': df_paradas, 
        'orden_paradas': df_orden_paradas, 'origen_destino_linea': df_origen_destino_linea
        }