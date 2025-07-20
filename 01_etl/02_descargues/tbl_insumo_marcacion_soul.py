
#LIBRERIAS
import pandas as pd
import sqlalchemy
from sqlalchemy.sql import text
from datetime import timedelta

exec(open(r'Y:\3.COMISIONES\Z_EQUIPO_COMISIONES\02_Karol_Horta\Z\conexion.py').read())
print("Entra")

CONSULTA1 = '''
SELECT 
    MONTH(fecha) AS MES,
    YEAR(fecha) AS ANHO,
    CONCAT(Documento, MONTH(fecha), YEAR(fecha) ) `KEY`,
    Fecha,
    Documento,
    IFNULL(Llamadas_ACD,0)  Llamadas_ACD,
    IFNULL(Retenido_Fija,0) Retenido_Fija,
    IFNULL(Retenido_Mov,0) Retenido_Mov,
    IFNULL(Promotores,0) Promotores,
    IFNULL(Detractores,0) Detractores,
    IFNULL(Neutros,0) Neutros,
    IFNULL(Total_Encuestas,0) Total_Encuestas,
    IFNULL(Mf_CAN1RE,0)   Mf_CAN1RE,
    IFNULL(Mf_CAN2RE,0)	 Mf_CAN2RE,
    IFNULL(Mf_CANCGN,0)  Mf_CANCGN,
    IFNULL(Mf_CANCNR,0) Mf_CANCNR,
    IFNULL(Mf_CANIEC,0)  Mf_CANIEC,
    IFNULL(Mf_CANMIN,0) Mf_CANMIN,
    IFNULL(Mf_CANSCS,0)  Mf_CANSCS,
    IFNULL(Mf_INFNIA,0) Mf_INFNIA,
    IFNULL(Mf_INFCDI,0) Mf_INFCDI,
    IFNULL(Mf_TTE,0) Mf_TTE,
    IFNULL(Mf_CTR,0)   Mf_CTR,
    IFNULL(Mf_LLC,0) Mf_LLC,
    IFNULL(Mf_Total,0) Mf_Total,
    IFNULL(Mm_Desactiv,0) Mm_Desactiv,
    IFNULL(Mm_Plan_par,0) Mm_Plan_par,
    IFNULL(Mm_Retenido,0) Mm_Retenido,
    IFNULL(Mm_total,0) Mm_total,
    IFNULL(Mm_Dime,0)  Mm_Dime

FROM bbdd_cos_bog_claro_retencion_convergente.tb_informe_nps_marcaciones_soul_retencion
WHERE
    MONTH(fecha) =  MONTH(DATE_ADD(CURDATE(), INTERVAL - 1 MONTH))
        AND YEAR(fecha) = YEAR(DATE_ADD(CURDATE(), INTERVAL - 1 MONTH))
		;
            '''
MATRIZ = pd.read_sql_query(CONSULTA1, conexion_wr61)
MATRIZ['FECHA_CARGUE']=pd.to_datetime('today')

MATRIZ.columns = MATRIZ.columns.str.upper()


name='bbdd_comisiones_retencion_inb_conv'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, ipADI, name))

MATRIZ.to_sql(con=database_connection, name='tbl_insumo_marcacion_soul', if_exists='append', index=False, chunksize=1000)
print(MATRIZ.head)
print('MARCACIONES DESCARGADAS')


