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

ruta_archivos = fr'L:\COMISIONES\CLARO\3_bbdd_comisiones_retencion_inb_conv\2_Insumos\{anho}\{inicio_mes}{mes}_{meses[mes]}\tbl_insumo_renegociaciones\*.xlsx'
archivos = glob.glob(ruta_archivos)
tbl_insumo_renegociacion_puro = pd.DataFrame()

for archivo in archivos:
    tbl_insumo_renegociacion_puro = pd.read_excel(archivo, sheet_name='Hoja1', skiprows=0)

    columnas_requeridas = ['Cuenta', 'Cierre', 'Documento Asesor', 'Fecha de creación']
    tbl_insumo_renegociacion_puro = tbl_insumo_renegociacion_puro[columnas_requeridas]

tbl_insumo_renegociacion_puro = tbl_insumo_renegociacion_puro.rename(columns={'Fecha de creación':'FECHA_DE_CREACION'}) 
tbl_insumo_renegociacion_puro.columns = tbl_insumo_renegociacion_puro.columns.str.upper()
        
tbl_insumo_renegociacion_puro.columns = tbl_insumo_renegociacion_puro.columns.str.replace(" ", "_")
tbl_insumo_renegociacion_puro['MES'] = mes
tbl_insumo_renegociacion_puro['ANHO'] = anho

print('Buscando documentos a penalizar')
#PARA CREAR EL DOCUMENTO PENAIZADOR
name='bbdd_claro_retencion_convergente_modelo_03'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, 
                                                       ipADI, name))

TB_HC = pd.read_sql_table('tbl_insumo_bhogar',con=database_connection)
TB_HC['CUENTA'].dtypes
tbl_insumo_renegociacion_puro['CUENTA']

documentos = tbl_insumo_renegociacion_puro.merge(TB_HC,how='inner',left_on='CUENTA', right_on='CUENTA',indicator=True)
print(documentos.columns)

doc_map = documentos.drop_duplicates(subset='CUENTA', keep='first').set_index('CUENTA')['DOCUMENTO_ASESOR']
print(doc_map)


tbl_insumo_renegociacion_puro['DOCUMENTO_PENALIZADO'] = tbl_insumo_renegociacion_puro['CUENTA'].map(doc_map).fillna(0)
tbl_insumo_renegociacion_puro['DOCUMENTO_PENALIZADO'] = tbl_insumo_renegociacion_puro['DOCUMENTO_PENALIZADO'].astype(int)
tbl_insumo_renegociacion_puro['FECHA_CARGUE'] = pd.to_datetime('today')
tbl_insumo_renegociacion_puro['FECHA_CARGUE'] = tbl_insumo_renegociacion_puro['FECHA_CARGUE'].dt.date
print(tbl_insumo_renegociacion_puro.columns)



#### CREACION DE tbl_insumo_renegociaciones_descto ####
#
#
#
#
#
renegociones_dcto = (
    tbl_insumo_renegociacion_puro[tbl_insumo_renegociacion_puro['DOCUMENTO_PENALIZADO'].notnull()]
    .groupby(['DOCUMENTO_PENALIZADO', 'FECHA_DE_CREACION', 'MES', 'ANHO'], as_index=False)
    .agg(PENALIZACION_RENEGOCIACIONES=('DOCUMENTO_PENALIZADO', 'count'))
    .rename(columns={'FECHA_DE_CREACION': 'FECHA'})
)

renegociones_dcto['KEY'] = renegociones_dcto['DOCUMENTO_PENALIZADO'].astype(str) + renegociones_dcto['MES'].astype(str) + renegociones_dcto['ANHO'].astype(str)

ORDENAR = [ 'MES', 'ANHO', 'KEY' ,'DOCUMENTO_PENALIZADO', 'FECHA', 'PENALIZACION_RENEGOCIACIONES']
tbl_insumo_renegociaciones_descto = renegociones_dcto[ORDENAR]

print(tbl_insumo_renegociaciones_descto.head)
#
#
#
#
#
#
#
#
#### CREACION DE tbl_insumo_renegociaciones_gestores  ####
#
#
#
# 
# 
# 
# 
renegociaciones_gestores = (
    tbl_insumo_renegociacion_puro[tbl_insumo_renegociacion_puro['CIERRE'].str.upper() == 'RETENIDO']
    .groupby(['DOCUMENTO_ASESOR', 'FECHA_DE_CREACION', 'MES', 'ANHO'], as_index=False)
    .agg(RETENIDOS=('DOCUMENTO_ASESOR', 'count'))
)

renegociaciones_gestores['KEY'] = renegociaciones_gestores['DOCUMENTO_ASESOR'].astype(str) + renegociaciones_gestores['MES'].astype(str) + renegociaciones_gestores['ANHO'].astype(str)

ORDENAR = [ 'MES', 'ANHO', 'KEY' ,'DOCUMENTO_ASESOR', 'FECHA_DE_CREACION', 'RETENIDOS']

tbl_insumo_renegociaciones_gestores = renegociaciones_gestores[ORDENAR]
print(tbl_insumo_renegociaciones_gestores.head())

#
#
#
#
#
#
name='bbdd_comisiones_retencion_inb_conv'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, ipADI, name))

#CARGUE tbl_insumo_renegociaciones_descto
tbl_insumo_renegociaciones_descto.to_sql(con=database_connection, name='tbl_insumo_renegociaciones_descto', if_exists='append', index=False, chunksize=1000)
print('tbl_insumo_renegociaciones_descto CARGADO')

#CARGUE tbl_insumo_renegociaciones_gestores
tbl_insumo_renegociaciones_gestores.to_sql(con=database_connection, name='tbl_insumo_renegociaciones_gestores', if_exists='append', index=False, chunksize=1000)
print('INSUMOS DE BASES RENEGOCIACIONES CARGADOS')

