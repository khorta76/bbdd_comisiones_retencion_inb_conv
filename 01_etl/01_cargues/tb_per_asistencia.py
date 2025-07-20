import pandas as pd
import numpy as np
import glob
import datetime
from sqlalchemy.sql import text
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

mes = (datetime.now() - relativedelta(months=1)).month
anho = (datetime.now() - relativedelta(months=1)).year

exec(open(r'Y:\3.COMISIONES\Z_EQUIPO_COMISIONES\02_Karol_Horta\Z\conexion.py').read())



CONSULTA1 = f'''
SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_hc
 WHERE MES = {mes}
	    AND ANHO = {anho};

'''
tbl_insumo_hc = pd.read_sql(CONSULTA1, conexion_adi.connect())

CONSULTA2 = f'''
SELECT * FROM x_informe_comisiones.tbl_cos_conexion_224_calculado
 WHERE MES = {mes}
	    AND ANHO = {anho};

'''
tbl_cos_conexion_224 = pd.read_sql(CONSULTA2, conexion_adi.connect())

# merge por DOCUMENTO, MES y ANHO
tb_per_asistencia = pd.merge(
    tbl_insumo_hc,
    tbl_cos_conexion_224,
    on=['DOCUMENTO', 'MES', 'ANHO'],
    how='left'
)

# Reemplaza NaN por 0 en las columnas necesarias
tb_per_asistencia['Tiempo_Real_conexion'] = tb_per_asistencia['Tiempo_Real_conexion'].fillna(0)
tb_per_asistencia['TIEMPO_OBJETIVO'] = tb_per_asistencia['TIEMPO_OBJETIVO'].fillna(0)

# Calcula la columna de división
tb_per_asistencia['PER_ASIST'] = tb_per_asistencia['Tiempo_Real_conexion'] / tb_per_asistencia['TIEMPO_OBJETIVO'] * 100
tb_per_asistencia['PER_ASIST'] = tb_per_asistencia['PER_ASIST'].fillna(0)

tb_per_asistencia['KEY'] = tb_per_asistencia['DOCUMENTO'].astype(str) + tb_per_asistencia['MES'].astype(str) + tb_per_asistencia['ANHO'].astype(str)

REQUERIDAS = [ 'MES', 'ANHO', 'DOCUMENTO', 'Tiempo_Real_conexion','TIEMPO_OBJETIVO', 'PER_ASIST', ]
tb_per_asistencia = tb_per_asistencia[REQUERIDAS]

name='bbdd_comisiones_retencion_inb_conv'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, 
                                                       ipADI, name))

tb_per_asistencia.to_sql(con=database_connection, name='tb_per_asistencia', if_exists='append', index=False, chunksize=1000)
print(tb_per_asistencia.head)
print('INSUMO PER ASISTENCIA CREADO')
