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

ruta_archivos = fr'L:\COMISIONES\CLARO\3_bbdd_comisiones_retencion_inb_conv\2_Insumos\{anho}\{inicio_mes}{mes}_{meses[mes]}\tbl_homologacion_ajustes\*.xlsx'
archivos = glob.glob(ruta_archivos)
print('INGRESO A RUTA AJUSTES')

tbl_homologacion_ajustes = pd.DataFrame()

for archivo in archivos:
    tbl_homologacion_ajustes = pd.read_excel(archivo, sheet_name='Hoja1', skiprows=0)
    print(archivo)

    columnas_requeridas = ['DOCUMENTO',	'VALOR']
    tbl_homologacion_ajustes = tbl_homologacion_ajustes[columnas_requeridas]

tbl_homologacion_ajustes['MES'] = mes
tbl_homologacion_ajustes['ANHO'] = anho

tbl_homologacion_ajustes['FECHA_CARGUE'] = pd.to_datetime('today')
tbl_homologacion_ajustes['KEY'] = tbl_homologacion_ajustes['DOCUMENTO'].astype(str) + tbl_homologacion_ajustes['MES'].astype(str) + tbl_homologacion_ajustes['ANHO'].astype(str)

ORDENAR = ['MES', 'ANHO' ,'KEY', 'DOCUMENTO','VALOR','FECHA_CARGUE'  ]

tbl_homologacion_ajustes = tbl_homologacion_ajustes[ORDENAR]

name='bbdd_comisiones_retencion_inb_conv'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, ipADI, name))

tbl_homologacion_ajustes.to_sql(con=database_connection, name='tbl_homologacion_ajustes', if_exists='append', index=False, chunksize=1000)
print(tbl_homologacion_ajustes.head)
print('INSUMO HOMOLOGACION AJUSTES CARGADO')
