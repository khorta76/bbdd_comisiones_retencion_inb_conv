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
print("INGRESO RUTA HC")
ruta_archivos = fr'L:\COMISIONES\CLARO\3_bbdd_comisiones_retencion_inb_conv\2_Insumos\{anho}\{inicio_mes}{mes}_{meses[mes]}\tbl_insumo_hc\*.xlsx'
archivos = glob.glob(ruta_archivos)

tbl_insumo_hc = pd.DataFrame()

for archivo in archivos:
    tbl_insumo_hc = pd.read_excel(archivo, sheet_name='Retencion', skiprows=0)
    tbl_insumo_hc = tbl_insumo_hc.rename(columns=lambda x:x.strip())
    print(archivo)

    columnas_requeridas = ['Tipo de Documento', 'Documento', 'Nombre', 'Cargo', 'Segmento', 'Coordinador', 
                           'Fecha Ingreso', 'Estado', 'Super Senior', 'Antigüedad[Meses]',
                            'Tipo Antigüedad', 'Usuario RR', 'Usuario AC']
    
    tbl_insumo_hc = tbl_insumo_hc[columnas_requeridas]  

    tbl_insumo_hc = tbl_insumo_hc.rename(columns={'Coordinador': 'NOMBRE_SUPERVISOR',
                                                  'Super Senior': 'NOMBRE_JEFE_DE_OPERACION',
                                                  'Antigüedad[Meses]':'ANTIGUEDADMES',
                                                  'Tipo Antigüedad':'ANTIGUEDAD'
                                                  })
    tbl_insumo_hc.columns = tbl_insumo_hc.columns.str.replace(" ", "_")
    tbl_insumo_hc.columns = tbl_insumo_hc.columns.str.upper() 

tbl_insumo_hc = tbl_insumo_hc[tbl_insumo_hc['ESTADO'] == 'Activo']
tbl_insumo_hc['MES']= mes
tbl_insumo_hc['ANHO']= anho
tbl_insumo_hc['FECHA_CARGUE'] = pd.to_datetime('today')
tbl_insumo_hc['KEY'] = tbl_insumo_hc['DOCUMENTO'].astype(str) + tbl_insumo_hc['MES'].astype(str) + tbl_insumo_hc['ANHO'].astype(str)

ORDENAR = ['MES', 'ANHO', 'KEY','DOCUMENTO', 'NOMBRE', 'CARGO', 'SEGMENTO',
           'NOMBRE_SUPERVISOR', 'FECHA_INGRESO', 'ESTADO',
            'NOMBRE_JEFE_DE_OPERACION', 'ANTIGUEDADMES', 'ANTIGUEDAD', 'USUARIO_RR',
            'USUARIO_AC', 'TIPO_DE_DOCUMENTO','FECHA_CARGUE']

tbl_insumo_hc = tbl_insumo_hc[ORDENAR]

name='bbdd_comisiones_retencion_inb_conv'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, 
                                                       ipADI, name))

tbl_insumo_hc.to_sql(con=database_connection, name='tbl_insumo_hc', if_exists='append', index=False, chunksize=1000)
print(tbl_insumo_hc.head)
print('INSUMO HC CARGADO')
