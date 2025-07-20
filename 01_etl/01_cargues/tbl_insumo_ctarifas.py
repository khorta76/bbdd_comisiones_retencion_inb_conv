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

ruta_archivos = fr'L:\COMISIONES\CLARO\3_bbdd_comisiones_retencion_inb_conv\2_Insumos\{anho}\{inicio_mes}{mes}_{meses[mes]}\tbl_insumo_ctarifas\*.xlsx'
archivos = glob.glob(ruta_archivos)
print('INGRESO A RUTA C TARIFAS')

tbl_insumo_ctarifas = pd.DataFrame()

for archivo in archivos:
    tbl_insumo_ctarifas = pd.read_excel(archivo, sheet_name='Hoja1', skiprows=0)
    print(archivo)

    columnas_requeridas = ['FECHA','DOC']
    tbl_insumo_ctarifas = tbl_insumo_ctarifas[columnas_requeridas]

    tbl_insumo_ctarifas.columns = tbl_insumo_ctarifas.columns.str.upper()

tbl_insumo_ctarifas['MES'] = mes
tbl_insumo_ctarifas['ANHO'] = anho


# Agrupación por DOC y FECHA con conteo por DOC
agrupado = (
    tbl_insumo_ctarifas
    .groupby(['DOC', 'FECHA','MES', 'ANHO'], as_index=False)
    .agg(CANTIDAD=('DOC', 'count'))
)


tbl_insumo_ctarifas = agrupado

tbl_insumo_ctarifas['FECHA_CARGUE'] = pd.to_datetime('today')
tbl_insumo_ctarifas['KEY'] = tbl_insumo_ctarifas['DOC'].astype(str) + tbl_insumo_ctarifas['MES'].astype(str) + tbl_insumo_ctarifas['ANHO'].astype(str)


ORDENAR = ['MES', 'ANHO' ,'KEY', 'DOC','FECHA', 'CANTIDAD', 'FECHA_CARGUE'  ]

tbl_insumo_ctarifas = tbl_insumo_ctarifas[ORDENAR]

name='bbdd_comisiones_retencion_inb_conv'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, ipADI, name))

tbl_insumo_ctarifas.to_sql(con=database_connection, name='tbl_insumo_ctarifas', if_exists='append', index=False, chunksize=1000)
print(tbl_insumo_ctarifas.head)
print('INSUMO BASE CTARIFAS CARGADO')
