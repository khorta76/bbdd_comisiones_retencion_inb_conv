import pandas as pd
import sqlalchemy
from sqlalchemy.sql import text

#REALIZA LA CONEXIÓN AL SERVIDOR DECLARADA EN CONEXION.PY 
exec(open(r'Y:\3.COMISIONES\Z_EQUIPO_COMISIONES\02_Karol_Horta\Z\conexion.py').read())
print("Entra")

CONSULTA1 = '''
 SELECT 
    MONTH(FECHA) AS MES,
    YEAR(FECHA) AS ANHO,
    CONCAT(Cedula, MONTH(FECHA), YEAR(FECHA)) `KEY`,
    FECHA,
    Cedula AS DOCUMENTO,
    SUM(Colocaciones_Hogar) Colocaciones_Hogar,
    SUM(Colocaciones_Movil) Colocaciones_Movil,
    IF(((SUM(Colocaciones_Hogar) + SUM(Colocaciones_Movil)) / SUM(LLamadas) * 100) IS NULL,
        0,
        ((SUM(Colocaciones_Hogar) + SUM(Colocaciones_Movil)) / SUM(LLamadas) * 100)) AS COLOCACION,
    IF(SUM(LLamadas) IS NULL,
        0,
        SUM(LLamadas)) AS LLamadas,
    IF(IF(((SUM(Colocaciones_Hogar) + SUM(Colocaciones_Movil)) - SUM(LLamadas)) < 0,
            0,
            ((SUM(Colocaciones_Hogar) + SUM(Colocaciones_Movil)) - SUM(LLamadas))) IS NULL,
        0,
        IF(((SUM(Colocaciones_Hogar) + SUM(Colocaciones_Movil)) - SUM(LLamadas)) < 0,
            0,
            ((SUM(Colocaciones_Hogar) + SUM(Colocaciones_Movil)) - SUM(LLamadas)))) AS SIN_MARCA
FROM
    bbdd_cos_bog_claro_retencion_convergente.tb_informe_colocaciones_retencion_general
WHERE
    MONTH(FECHA) = MONTH(DATE_ADD(CURDATE(), INTERVAL - 1 MONTH))
        AND YEAR(FECHA) = YEAR(DATE_ADD(CURDATE(), INTERVAL - 1 MONTH))
        AND Cedula IS NOT NULL
GROUP BY CEDULA , MES , ANHO

                        '''

MATRIZ = pd.read_sql_query(CONSULTA1, conexion_wr61)

print("Data Descargada")
MATRIZ.columns = MATRIZ.columns.str.upper() 

MATRIZ['FECHA_CARGUE']=pd.to_datetime('today')

name='bbdd_comisiones_retencion_inb_conv'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                format(userADI, passwordADI, 
                                                        ipADI, name))

MATRIZ.to_sql(con=database_connection, name='tbl_insumo_colocaciones', if_exists='append',index=False, chunksize=1000)
print(MATRIZ.head)
print("Data colocaciones cargada")
