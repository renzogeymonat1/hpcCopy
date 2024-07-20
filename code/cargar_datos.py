import pandas as pd

# ubicacion de archivos
VIAJES = 'C:\\Users\\renzo\\Desktop\\hpc\\csv\\ROkzNjh5SCO63dSgeX8tcw2.csv'
PARADAS = 'C:\\Users\\renzo\\Desktop\\hpc\\csv\\v_uptu_paradas.csv'
ORIGEN_DESTINO_LINEAS = 'C:\\Users\\renzo\\Desktop\\hpc\\csv\\v_uptu_lsv.csv'
ORDEN_PARADAS = 'C:\\Users\\renzo\\Desktop\\hpc\\csv\\uptu_pasada_variante.csv'

def get_datasets():
    # Leer el archivo CSV y cargar los primeros un millón de registros
    df_viajes = pd.read_csv(VIAJES)
    df_paradas = pd.read_csv(PARADAS, delimiter=';')
    df_origen_destino_linea = pd.read_csv(ORIGEN_DESTINO_LINEAS, delimiter=';')
    df_orden_paradas = pd.read_csv(ORDEN_PARADAS, delimiter=';')

    df_origen_destino_linea = df_origen_destino_linea.drop_duplicates(subset=['DESC_LINEA', 'DESC_VARIA'])

    return { 
        'viajes': df_viajes, 'paradas': df_paradas, 
        'orden_paradas': df_orden_paradas, 'origen_destino_linea': df_origen_destino_linea
        }