#LIBRERIAS
import pandas as pd
import sqlalchemy
from sqlalchemy.sql import text
from datetime import timedelta

exec(open(r'Y:\3.COMISIONES\Z_EQUIPO_COMISIONES\02_Karol_Horta\Z\conexion.py').read())
print("Entra")

CONSULTA1 = '''
 SELECT 
 MONTH(ROW_DATE) AS MES,
    YEAR(ROW_DATE) AS ANHO,
    CONCAT(Login_ID, MONTH(ROW_DATE), YEAR(ROW_DATE) ) `KEY`,
    ROW_DATE AS FECHA,
    Login_ID AS LOGIN_ID,
    SUM(ACDCALLS) AS LLAMADAS_ACD,
    SUM(AUXOUTCALLS) AS AUXOUTCALLS
    
FROM
    bbdd_cos_bog_claro_retencion_convergente.tb_auxiliar_depurado_completo
WHERE
    YEAR(ROW_DATE) = YEAR(DATE_ADD(CURDATE(), INTERVAL - 1 MONTH))
        AND MONTH(ROW_DATE) = MONTH(DATE_ADD(CURDATE(), INTERVAL - 1 MONTH))
GROUP BY Login_ID , ROW_DATE

        '''

MATRIZ = pd.read_sql_query(CONSULTA1, conexion_wr61)

MATRIZ['FECHA_CARGUE']=pd.to_datetime('today')

name='bbdd_comisiones_retencion_inb_conv'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, ipADI, name))

MATRIZ.to_sql(con=database_connection, name='tbl_insumo_llamadas_outhb', if_exists='append', index=False, chunksize=1000)
print(MATRIZ.head)
print('LLAMADAS OUTH DESCARGADAS')



