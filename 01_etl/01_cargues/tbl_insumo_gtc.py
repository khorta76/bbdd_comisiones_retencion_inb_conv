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
print("INGRESO RUTA GTC")
ruta_archivos = fr'L:\COMISIONES\CLARO\3_bbdd_comisiones_retencion_inb_conv\2_Insumos\{anho}\{inicio_mes}{mes}_{meses[mes]}\tbl_insumo_gtc\*.xlsx'
archivos = glob.glob(ruta_archivos)

tbl_insumo_gtc = pd.DataFrame()
for archivo in archivos:
    tbl_insumo_gtc = pd.read_excel(archivo, sheet_name='Hoja1', skiprows=0)
    tbl_insumo_gtc.columns = tbl_insumo_gtc.columns.str.upper()
    columnas_requeridas = ['FECHA_GTC','USUARIO_RET']
    tbl_insumo_gtc = tbl_insumo_gtc[columnas_requeridas]
    tbl_insumo_gtc = tbl_insumo_gtc.rename(columns={'USUARIO_RET':'USUARIO'})

tbl_insumo_gtc['MES'] = mes
tbl_insumo_gtc['ANHO'] = anho  
tbl_insumo_gtc['FECHA_CARGUE'] = pd.to_datetime('today')
tbl_insumo_gtc['KEY'] = tbl_insumo_gtc['USUARIO'].astype(str) + tbl_insumo_gtc['MES'].astype(str) + tbl_insumo_gtc['ANHO'].astype(str)

ORDENAR = [ 'MES', 'ANHO','KEY', 'FECHA_GTC', 'USUARIO']

tbl_insumo_gtc = tbl_insumo_gtc[ORDENAR]

name='bbdd_comisiones_retencion_inb_conv'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, 
                                                       ipADI, name))

tbl_insumo_gtc.to_sql(con=database_connection, name='tbl_insumo_gtc', if_exists='append', index=False, chunksize=1000)
print(tbl_insumo_gtc.head)
print('INSUMO GTC CARGADO')

