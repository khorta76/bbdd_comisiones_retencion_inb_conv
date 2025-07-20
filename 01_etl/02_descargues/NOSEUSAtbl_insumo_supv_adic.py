import pandas as pd
import sqlalchemy
from sqlalchemy.sql import text

#REALIZA LA CONEXIÓN AL SERVIDOR DECLARADA EN CONEXION.PY 
exec(open(r'Y:\3.COMISIONES\Z_EQUIPO_COMISIONES\02_Karol_Horta\Z\conexion.py').read())
print("Entra")
#REALIZA LA EL QUERY EN MYSQL EN EL SERVIDOR DE WORFORCE
CONSULTA1 = '''
              SELECT 

    MONTH(MSM_FECHA) AS MES,
    YEAR(MSM_FECHA) AS ANHO,
    CONCAT(Documento, MONTH(MSM_FECHA), YEAR(MSM_FECHA) ) `KEY`,
    Documento,
    PRODUCTO,
    ESTADO
FROM
    bbdd_cos_bog_claro_retencion_convergente.tb_informe_servicios_adicionales_retencion
WHERE
    MONTH(MSM_FECHA) = MONTH(DATE_ADD(CURDATE(), INTERVAL - 1 MONTH))
        AND YEAR(MSM_FECHA) = YEAR(DATE_ADD(CURDATE(), INTERVAL - 1 MONTH))
        AND PRODUCTO IN ('WINSPORT+' , 'CLARO_UP', 'MAX')
                                        '''


MATRIZ = pd.read_sql_query(CONSULTA1, conexion_wr61)
print("Data Descargada")

MATRIZ.columns = MATRIZ.columns.str.upper() 

MATRIZ['FECHA_CARGUE']=pd.to_datetime('today')

name='bbdd_comisiones_retencion_inb_conv'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                format(userADI, passwordADI, 
                                                        ipADI, name))

MATRIZ.to_sql(con=database_connection, name='tbl_insumo_supv_adic', if_exists='append',index=False, chunksize=1000)
print(MATRIZ.head)
print("Data adicionales cargada")
