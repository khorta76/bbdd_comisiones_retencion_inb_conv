import pandas as pd
import sqlalchemy
from sqlalchemy.sql import text


exec(open(r'Y:\3.COMISIONES\Z_EQUIPO_COMISIONES\02_Karol_Horta\Z\conexion.py').read())
print("Entra")

CONSULTA1 = '''
           SELECT 
    MONTH(FECHA) AS MES,
    YEAR(FECHA) AS ANHO,
	CONCAT(Documento,MONTH(FECHA),YEAR(FECHA)) `KEY`,
    Fecha,
    Documento,
    Usuario_RR,
    Llm_Inb,
    Llm_Out,
    Ratio_Inb,
    Ratio_Out,
    T_Programado,
    T_Asiste,
    Programado,
    Asistencia,
    T_Retardo,
    Tinh_Ft,
    Tinh_it,
    Tinh_Alm,
    Tinh_Break,
    Tinh_Capa,
    /* NPS_Encuestas,
    NPS_Promotores,
    NPS_Detractores,
    NPS_Neutros, */ 
    Intenciones_movil,
    Retenciones_movil,
    No_retenciones_movil,
    Intenciones_fija,
    Retenciones_fija,
    No_retenciones_fija,
    Can_serv_fija,
    Can_serv_reten_fija
FROM bbdd_cos_bog_claro_retencion_convergente.tb_informe_operativo_claro_convergente
WHERE   YEAR(fecha) = YEAR(DATE_ADD(CURDATE(), INTERVAL - 1 MONTH))
        AND MONTH(fecha) = MONTH(DATE_ADD(CURDATE(), INTERVAL - 1 MONTH))
        ;
              
                                          
                                                            '''

#LEE LA CONSULTA1 REALIZADA Y LA EXPORTA A EXCEL
MATRIZ = pd.read_sql_query(CONSULTA1, conexion_wr61)
print("Data Descargada")

MATRIZ.columns = MATRIZ.columns.str.upper() 

MATRIZ['FECHA_CARGUE']=pd.to_datetime('today')

name='bbdd_comisiones_retencion_inb_conv'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                format(userADI, passwordADI, 
                                                        ipADI, name))

MATRIZ.to_sql(con=database_connection, name='tbl_insumo_inf_op', if_exists='append',index=False, chunksize=1000)
print(MATRIZ.head)
print("Data Cargada")
