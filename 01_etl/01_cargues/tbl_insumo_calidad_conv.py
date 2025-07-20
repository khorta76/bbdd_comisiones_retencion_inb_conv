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

ruta_archivos = fr'L:\COMISIONES\CLARO\3_bbdd_comisiones_retencion_inb_conv\2_Insumos\{anho}\{inicio_mes}{mes}_{meses[mes]}\tbl_insumo_calidad\*.xlsx'
archivos = glob.glob(ruta_archivos)

tbl_insumo_calidad_conv=pd.DataFrame()

for archivo in archivos:
    tbl_insumo_calidad_conv= pd.read_excel(archivo, sheet_name='BBDD', skiprows=0)
    columnas_requeridas = ['Documento Asesor Auditado','Validación','Calificación Del Monitoreo','Ojt', 'Nombre Matriz']

    tbl_insumo_calidad_conv = tbl_insumo_calidad_conv[columnas_requeridas]
    
    tbl_insumo_calidad_conv = tbl_insumo_calidad_conv.rename(columns={'Documento Asesor Auditado': 'DOCUMENTO',
                                                                      'Validación':'VALIDACION',
                                                                      'Calificación Del Monitoreo': 'NOTA_CALIDAD',
                                                                       'Nombre Matriz': 'Nombre_Matriz'})

    tbl_insumo_calidad_conv.columns = tbl_insumo_calidad_conv.columns.str.upper()

tbl_insumo_calidad_conv['MES']= mes
tbl_insumo_calidad_conv['ANHO']= anho

# Filtrar y agrupar 
insumo_agrupado = (
    tbl_insumo_calidad_conv[
        (tbl_insumo_calidad_conv['VALIDACION'] == 1) &
        (tbl_insumo_calidad_conv['OJT'].str.upper() == 'NO')
    ]
    .groupby(['DOCUMENTO', 'MES', 'ANHO'], as_index=False)
    .agg(NOTA_CALIDAD=('NOTA_CALIDAD', 'mean'))
    
)

insumo_agrupado['KEY'] = insumo_agrupado['DOCUMENTO'].astype(str) + insumo_agrupado['MES'].astype(str) + insumo_agrupado['ANHO'].astype(str)
insumo_agrupado['FECHA_CARGUE'] = pd.to_datetime('today')

ORDENAR = ['MES', 'ANHO', 'KEY', 'DOCUMENTO', 'NOTA_CALIDAD' ,'FECHA_CARGUE' ]

tbl_insumo_calidad_conv = insumo_agrupado[ORDENAR]

name='bbdd_comisiones_retencion_inb_conv'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, 
                                                       ipADI, name))


tbl_insumo_calidad_conv.to_sql(con=database_connection, name='tbl_insumo_calidad_conv', if_exists='append', index=False, chunksize=1000)
print(tbl_insumo_calidad_conv.head)
print('INSUMO BASE CALIDAD CARGADO')

