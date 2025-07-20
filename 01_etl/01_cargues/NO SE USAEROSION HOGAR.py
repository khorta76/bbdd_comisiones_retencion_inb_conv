import pandas as pd
import os
from pathlib import Path
import glob
import datetime
import dateutil.relativedelta
import sqlalchemy
from sqlalchemy.sql import text
from dateutil.relativedelta import relativedelta

exec(open(r'C:\Users\karol.orta\Pictures\Z_COPIAS_CARGUES3\conexion.py').read())

ruta_archivos = r'Y:\3.COMISIONES\CLARO\3_RETENCION_CONVERGENTE\1_PROYECTO PYTHON_MODELO_2\0_BBDD_CARGUE\BASE_EROSION_HOGAR\*.xlsx'
archivos = glob.glob(ruta_archivos)

tbl_BBDD = pd.DataFrame()

for archivo in archivos:
    tbl_BBDD_COS = pd.read_excel(archivo, sheet_name='BBDD', skiprows=0)
    print(archivo)

    columnas_requeridas = ['Cuenta / Min','Segmento','Tipo de base','Marcación','Fecha','Usuario','Observación','Observación 2',
                           'Gestion','# Base','Fecha recepción','Nombre base','Duplicidad','Llave','Llave aux','Fecha asignación',
                           '# Asignación','Documento','Asesor','Fecha de gestión','Resultado general','Consolidado / Submotivos resultado general',
                          ]

    tbl_BBDD_COS = tbl_BBDD_COS[columnas_requeridas]

    tbl_BBDD_COS = tbl_BBDD_COS.rename(columns={'Cuenta / Min':'Cuenta_Min',
                                                'Tipo de base':'Tipo_base',
                                                '# Base':'Numero_Base',
                                                'Nombre base':'Nombre_base',
                                                'Fecha asignación':'Fecha_asignacion',
                                                '# Asignación':'Asignación',
                                                'Documento':'Documento',
                                                'Asesor':'NOMBRES_APELLIDOS',
                                                'Fecha de gestión':'Fecha_gestion',
                                                'Resultado general':'Resultado_general',
                                                'Consolidado / Submotivos resultado general':'submotivo_resultado_general'
                                               })

    tbl_BBDD = pd.concat([tbl_BBDD, tbl_BBDD_COS], ignore_index=True)
print(tbl_BBDD.head)

tbl_BBDD['Fecha'] = pd.to_datetime(tbl_BBDD['Fecha'])

# Crear columna de mes en ambos DataFrames
tbl_BBDD['MES'] = tbl_BBDD['Fecha'].dt.month

# Crear columna de año en ambos DataFrames
tbl_BBDD['AÑO'] = tbl_BBDD['Fecha'].dt.year

tbl_BBDD['Fecha'] = (tbl_BBDD['Fecha']).dt.date

# Agregar columna de fecha de carga
tbl_BBDD['FECHA_CARGUE'] = pd.to_datetime('today')


name='bbdd_claro_retencion_convergente_modelo_02'

database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, 
                                                       ipADI, name))

tbl_BBDD.to_sql(con=database_connection, name='tbl_insumo_erosion_hogar', if_exists='append', index=False, chunksize=1000)
print(tbl_BBDD.head)
print('CONECTADO')



