import pandas as pd
import numpy as np
import glob
import datetime
from sqlalchemy.sql import text
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

exec(open(r'Y:\3.COMISIONES\Z_EQUIPO_COMISIONES\02_Karol_Horta\Z\conexion.py').read())

CONSULTA1 = '''
SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_bmovil

WHERE MES = 5 /* MONTH(date_add(curdate(), INTERVAL -1 MONTH)) */
AND ANHO = YEAR(date_add(curdate(), INTERVAL -1 MONTH))
;
'''


CONSULTA2 = '''
SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_bhogar
 WHERE MES = 5 /* MONTH(date_add(curdate(), INTERVAL -1 MONTH) */
AND ANHO = YEAR(date_add(curdate(), INTERVAL -1 MONTH))
;
'''
CONSULTA3 = '''
SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_colocaciones
 WHERE MES = 5 /* MONTH(date_add(curdate(), INTERVAL -1 MONTH) */
AND ANHO = YEAR(date_add(curdate(), INTERVAL -1 MONTH))
;
'''



tbl_insumo_bmovil = pd.read_sql_query(CONSULTA1, conexion_adi)
tbl_insumo_bhogar = pd.read_sql_query(CONSULTA2, conexion_adi)
tbl_insumo_colocaciones = pd.read_sql_query(CONSULTA3, conexion_adi)

print('QRYS EJECUTADOS')
#
#
#
###########GROUP BY
#
#
#
# 1. Subconsulta B: Hogar (retenidos)
hogar_retenidos = (
    tbl_insumo_bhogar[tbl_insumo_bhogar['ESTADO'] == 'Retenido']
    .groupby(['CEDULA', 'MES', 'ANHO'], as_index=False)
    .agg(RETENIDOS_HOGAR=('CAN_SERV_', 'sum'))
)
#
#
#
# 2. Subconsulta B: Hogar (No retenidos)
hogar_no_retenidos = (
    tbl_insumo_bhogar[tbl_insumo_bhogar['ESTADO'] == 'No retenido']
    .groupby(['CEDULA', 'MES', 'ANHO'], as_index=False)
    .agg(NO_RETENIDOS_HOGAR=('CEDULA', 'count'))
)
#
#
#
#
# 3. Subconsulta B: Hogar (total retenidos hogar)
#
#
#
hogar_tt_registros = (
    tbl_insumo_bhogar
    .groupby(['CEDULA', 'MES', 'ANHO'], as_index=False)
    .agg(TOTAL_REGISTROS_HOGAR=('CEDULA', 'count'))
)
#
#
#
#  4. Subconsulta C: Móvil (retenidos)
#
#
#
movil_retenidos = (
    tbl_insumo_bmovil[tbl_insumo_bmovil['COD_TICKLER']== 'RETENIDO']
    .groupby(['CEDULA', 'MES', 'ANHO'], as_index=False)
    .agg(RETENIDOS_MOVIL=('CEDULA', 'count'))
)
#
#
#
#
#  5. Subconsulta C: Móvil (No retenidos)
#
#
#
movil_no_retenidos = (
    tbl_insumo_bmovil[tbl_insumo_bmovil['COD_TICKLER'].str.contains('NO', na=False)]
    .groupby(['CEDULA', 'MES', 'ANHO'], as_index=False)
    .agg(NO_RETENIDOS_MOVIL=('CEDULA', 'count'))
)
#
#
#
#
# 6. Subconsulta C: Móvil (total retenidos)
#
#
#
movil_tt_registros = (
    tbl_insumo_bmovil
    .groupby(['CEDULA', 'MES', 'ANHO'], as_index=False)
    .agg(TOTAL_REGISTROS_MOVIL=('CEDULA', 'count'))
)

print('AGRUPACIONES OK')
#
#
#
######MERGE 
# 7. Unir todos los DataFrames por CEDULA, MES y ANHO
#
#
#
df_efectividad = hogar_retenidos.merge(
    hogar_no_retenidos, on=['CEDULA', 'MES', 'ANHO'], how='outer'
).merge(
    hogar_tt_registros, on=['CEDULA', 'MES', 'ANHO'], how='outer'
).merge(
    movil_retenidos, on=['CEDULA', 'MES', 'ANHO'], how='outer'
).merge(
    movil_no_retenidos, on=['CEDULA', 'MES', 'ANHO'], how='outer'
).merge(
    movil_tt_registros, on=['CEDULA', 'MES', 'ANHO'], how='outer'
)

# Opcional: llenar NaN con 0 para facilitar cálculos posteriores
df_efectividad = df_efectividad.fillna(0)

print('MERGES OK')

#
#
# 
#
# 8. FILTRAR COLOCACION MENOR A 85 PARA CARGAR DIF_LLAMADAS 
tbl_insumo_colocaciones_filtrado = tbl_insumo_colocaciones[tbl_insumo_colocaciones['COLOCACION'] < 85]

print(tbl_insumo_colocaciones_filtrado.head())

## 9. Agrupa SIN_MARCA por DOCUMENTO, MES, ANHO en colocaciones
sin_marca_agg = (
    tbl_insumo_colocaciones_filtrado
    .groupby(['DOCUMENTO', 'MES', 'ANHO'], as_index=False)
    .agg(SIN_MARCA=('SIN_MARCA', 'sum'))
)

# 10. Merge con df_efectividad (CEDULA <-> DOCUMENTO)
df_efectividad = df_efectividad.merge(
    sin_marca_agg,
    left_on=['CEDULA', 'MES', 'ANHO'],
    right_on=['DOCUMENTO', 'MES', 'ANHO'],
    how='left'
)
df_efectividad['SIN_MARCA'] = df_efectividad['SIN_MARCA'].fillna(0)

# (Opcional) Si no quieres la columna DOCUMENTO en el resultado final:
df_efectividad = df_efectividad.drop(columns=['DOCUMENTO'])

# 11. Calcula la columna EFECTIVIDAD
df_efectividad['EFECTIVIDAD'] = (
    (df_efectividad['RETENIDOS_HOGAR'] + df_efectividad['RETENIDOS_MOVIL']) /
    (df_efectividad['TOTAL_REGISTROS_HOGAR'] + df_efectividad['TOTAL_REGISTROS_MOVIL'] + df_efectividad['SIN_MARCA'])
) * 100

# 12. Limitar el resultado a 100 como máximo
df_efectividad['EFECTIVIDAD'] = df_efectividad['EFECTIVIDAD'].clip(upper=100)

df_efectividad['KEY'] = df_efectividad['CEDULA'].astype(str) + df_efectividad['MES'].astype(str) + df_efectividad['ANHO'].astype(str)
print(df_efectividad.columns)

ORDENAR = [ 'MES', 'ANHO', 'KEY', 'CEDULA', 'RETENIDOS_HOGAR', 'NO_RETENIDOS_HOGAR',
       'TOTAL_REGISTROS_HOGAR', 'RETENIDOS_MOVIL', 'NO_RETENIDOS_MOVIL',
       'TOTAL_REGISTROS_MOVIL', 'SIN_MARCA', 'EFECTIVIDAD', ]

df_efectividad = df_efectividad[ORDENAR]


name='bbdd_comisiones_retencion_inb_conv'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, ipADI, name))

df_efectividad.to_sql(con=database_connection, name='tb_insumo_efectividad_conv', if_exists='append', index=False, chunksize=1000)
print(df_efectividad.head())
print('INSUMO EFECTIVIDAD CONVERGENTE CREADO')
