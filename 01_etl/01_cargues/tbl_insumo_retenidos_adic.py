import pandas as pd
import numpy as np
import glob
import datetime
from sqlalchemy.sql import text
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

meses = {
    1: 'enero',
    2: 'febrero',
    3: 'marzo',
    4: 'abril',
    5: 'mayo',
    6: 'junio',
    7: 'julio',
    8: 'agosto',
    9: 'septiembre',
    10: 'octubre',
    11: 'noviembre',
    12: 'diciembre',
}

mes = (datetime.now() - relativedelta(months=1)).month
anho = (datetime.now() - relativedelta(months=1)).year

inicio_mes = '0' if mes < 10 else ''
exec(open(r'Y:\3.COMISIONES\Z_EQUIPO_COMISIONES\02_Karol_Horta\Z\conexion.py').read())


ruta_archivos = fr'L:\COMISIONES\CLARO\3_bbdd_comisiones_retencion_inb_conv\2_Insumos\{anho}\{inicio_mes}{mes}_{meses[mes]}\tbl_insumo_retenidos_adic\*.xlsx'
archivos = glob.glob(ruta_archivos)
tbl_insumo_retenidos_adic = pd.DataFrame()
for archivo in archivos:

    tbl_insumo_retenidos_adic = pd.read_excel(archivo, sheet_name='Retenidos Adicionales', skiprows=0)
    columnas_requeridas = [ 'MSM_USUARIO', 'ESTADO'	,'MSM_FECHA']

    tbl_insumo_retenidos_adic = tbl_insumo_retenidos_adic[columnas_requeridas]

tbl_insumo_retenidos_adic = tbl_insumo_retenidos_adic[tbl_insumo_retenidos_adic['ESTADO'] == 'RETENIDO']
tbl_insumo_retenidos_adic['MES'] = mes
tbl_insumo_retenidos_adic['ANHO'] = anho

# Agrupar 
retenidos_adc = (
    tbl_insumo_retenidos_adic
    .groupby(['MSM_USUARIO', 'MSM_FECHA', 'MES', 'ANHO'], as_index=False)
    .agg(RETENIDOS_ADC=('MSM_USUARIO', 'count'))
    .rename(columns={'MSM_FECHA': 'FECHA', 'MSM_USUARIO': 'USUARIO'})
)



tbl_insumo_retenidos_adic = retenidos_adc
# Convertir FECHA a datetime (detectando el formato día/mes/año)
tbl_insumo_retenidos_adic['FECHA'] = pd.to_datetime(tbl_insumo_retenidos_adic['FECHA'], dayfirst=True, errors='coerce')

tbl_insumo_retenidos_adic['FECHA_CARGUE'] = pd.to_datetime('today')
tbl_insumo_retenidos_adic['KEY'] = tbl_insumo_retenidos_adic['USUARIO'].astype(str) + tbl_insumo_retenidos_adic['MES'].astype(str) + tbl_insumo_retenidos_adic['ANHO'].astype(str)

ORDENAR = ['MES', 'ANHO','KEY','USUARIO', 'FECHA', 'RETENIDOS_ADC', 'FECHA_CARGUE']
tbl_insumo_retenidos_adic = tbl_insumo_retenidos_adic[ORDENAR]

name='bbdd_comisiones_retencion_inb_conv'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, 
                                                       ipADI, name))


tbl_insumo_retenidos_adic.to_sql(con=database_connection, name='tbl_insumo_retenidos_adic', if_exists='append', index=False, chunksize=1000)
print(tbl_insumo_retenidos_adic.head())
print('INSUMO RETENIDOS ADIC CARGADO')


