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
print("Entra")

ruta_archivos = r'Y:\3.COMISIONES\CLARO\3_RETENCION_CONVERGENTE\1_PROYECTO PYTHON_MODELO_2\0_BBDD_CARGUE\BASE_EROSION_MOVIL\*.xlsx'
archivos = glob.glob(ruta_archivos)

tbl_BBDD = pd.DataFrame()

for archivo in archivos:
    tbl_BBDD_COS = pd.read_excel(archivo, sheet_name='BBDD', skiprows=0)
    print(archivo)

    columnas_requeridas = ['Id', 'CO ID', 'Nombre Cliente', 'Corte', 'Plan Anterior', 'Plan Nuevo', 'Gestión', 'Cierre', 'Valor Plan Actualizado', 
                           'Motivo No Gestionable', 'Observación', 'Asesor', 'Documento Asesor', 'Fecha de creación', 'Fecha de actualización', 
                           'Fecha Homologada', 'Duplicidad', 'Cierre Homologado', 'Contacto Efectivo', 'Recuperado', 'Erosión Actual', 'Erosión Final',
                           'Conteo', 'Base Pertenece']

    tbl_BBDD_COS = tbl_BBDD_COS[columnas_requeridas]

    tbl_BBDD_COS = tbl_BBDD_COS.rename(columns={'Id': 'ID',
                                            'CO ID': 'CO ID',
                                            'Nombre Cliente': 'NOMBRE CLIENTE',
                                            'Corte': 'CORTE',
                                            'Plan Anterior': 'PLAN ANTERIOR',
                                            'Plan Nuevo': 'PLAN NUEVO',
                                            'Gestión': 'GESTIÓN',
                                            'Cierre': 'CIERRE',
                                            'Valor Plan Actualizado': 'VALOR PLAN ACTUALIZADO',
                                            'Motivo No Gestionable': 'MOTIVO NO GESTIONABLE',
                                            'Observación': 'OBSERVACIÓN',
                                            'Asesor': 'ASESOR',
                                            'Documento Asesor': 'DOCUMENTO ASESOR',
                                            'Fecha de creación': 'FECHA DE CREACIÓN',
                                            'Fecha de actualización': 'FECHA DE ACTUALIZACIÓN',
                                            'Fecha Homologada': 'FECHA HOMOLOGADA',
                                            'Duplicidad': 'DUPLICIDAD',
                                            'Cierre Homologado': 'CIERRE HOMOLOGADO',
                                            'Contacto Efectivo': 'CONTACTO EFECTIVO',
                                            'Recuperado': 'RECUPERADO',
                                            'Erosión Actual': 'EROSIÓN ACTUAL',
                                            'Erosión Final': 'EROSIÓN FINAL',
                                            'Conteo': 'CONTEO',
                                            'Base Pertenece': 'BASE PERTENECE'})


    tbl_BBDD = pd.concat([tbl_BBDD, tbl_BBDD_COS], ignore_index=True)
print(tbl_BBDD.head)
tbl_BBDD['FECHA_CARGUE'] = pd.to_datetime('today')


fecha_actual = datetime.date.today()
fecha_aplicada = fecha_actual - datetime.timedelta(days=60)
tbl_BBDD['FECHA_DE_PENALIZACION']= fecha_aplicada

name='bbdd_claro_retencion_convergente_modelo_02'

database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, 
                                                       ipADI, name))

# Cargar los datos en la tabla
tbl_BBDD.to_sql(con=database_connection, name='tbl_insumo_erosion_movil', if_exists='append', index=False, chunksize=1000)
print(tbl_BBDD.head)
print('CONECTADO')



