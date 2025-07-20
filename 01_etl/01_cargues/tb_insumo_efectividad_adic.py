import pandas as pd
import numpy as np
import glob
import datetime
from sqlalchemy.sql import text
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

exec(open(r'Y:\3.COMISIONES\Z_EQUIPO_COMISIONES\02_Karol_Horta\Z\conexion.py').read())

CONSULTA1 = '''
SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_celula_adic

WHERE MES = 5 /* MONTH(date_add(curdate(), INTERVAL -1 MONTH)) */
AND ANHO = YEAR(date_add(curdate(), INTERVAL -1 MONTH))
;
'''


tbl_insumo_celula_adic = pd.read_sql_query(CONSULTA1, conexion_adi)

print('QRY EJECUTADOS')
#
#
#
###########GROUP BY
#
#
#
# 1. Subconsulta  (retenidos)
retenidos = (
    tbl_insumo_celula_adic[tbl_insumo_celula_adic['ESTADO'] == 'RETENIDO']
    .groupby(['DOCUMENTO', 'MES', 'ANHO'], as_index=False)
    .agg(RETENIDOS=('DOCUMENTO', 'count'))
)
#
#
#
# 2. Subconsulta (totales)
totales = (
    tbl_insumo_celula_adic
    .groupby(['DOCUMENTO', 'MES', 'ANHO'], as_index=False)
    .agg(INTENCIONES=('DOCUMENTO', 'count'))
)
#
#
#

print('AGRUPACIONES OK')
#
#
#
# 3. Unir todos los DataFrames por CEDULA, MES y ANHO
#
#
#
df_efectividad = retenidos.merge(
    totales, on=['DOCUMENTO', 'MES', 'ANHO'], how='outer'
)

# Opcional: llenar NaN con 0 para facilitar cálculos posteriores
df_efectividad = df_efectividad.fillna(0)
print('MERGES OK')
#
#
#
#

# 8. Calcula la columna EFECTIVIDAD
df_efectividad['EFECTIVIDAD'] = (
    (df_efectividad['RETENIDOS'] / df_efectividad['INTENCIONES']) * 100
)

# 9. Limitar el resultado a 100 como máximo
df_efectividad['EFECTIVIDAD'] = df_efectividad['EFECTIVIDAD'].clip(upper=100)

df_efectividad['KEY'] = df_efectividad['DOCUMENTO'].astype(str) + df_efectividad['MES'].astype(str) + df_efectividad['ANHO'].astype(str)
print(df_efectividad.columns)

ORDENAR = [ 'MES', 'ANHO', 'KEY', 'DOCUMENTO', 'RETENIDOS', 'INTENCIONES', 'EFECTIVIDAD', ]

df_efectividad = df_efectividad[ORDENAR]

name='bbdd_comisiones_retencion_inb_conv'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, ipADI, name))

df_efectividad.to_sql(con=database_connection, name='tb_insumo_efectividad_adic', if_exists='append', index=False, chunksize=1000)
print(df_efectividad.head())
print('INSUMO EFECTIVIDAD ADIC CREADO')
