
#LIBRERIAS
import pandas as pd
import sqlalchemy
from sqlalchemy.sql import text
from datetime import timedelta


# Conexion al servidor
exec(open(r'Y:\3.COMISIONES\Z_EQUIPO_COMISIONES\02_Karol_Horta\Z\conexion.py').read())
print("Entra")

#REALIZA LA EL QUERY EN MYSQL EN EL SERVIDOR DE WORFORCE
CONSULTA1 = '''

                        SELECT 
                        N_Mes as Mes,
                        Anho,
                        concat(Documento, N_Mes , Anho) `KEY`,
                        Documento,
                        Fecha,
                        programado,
                        asistencia_soul,
                        ausencias_soul,
                        IFNULL(novedad,0) novedad,
                        tiempo_programdo_real,
                        tiempo_logueo_soul_real
                        
                        FROM bbdd_config.tb_informe_cos_conexion
                        WHERE N_MES =  MONTH(date_add(curdate(), INTERVAL -1 MONTH))
                        AND ANHO = YEAR(date_add(curdate(), INTERVAL -1 MONTH))
                        AND Campana  IN ('Claro - Convergencia Retencion' , 'Transversales Backoffice')
                        ;
        '''

#LEE LA CONSULTA1 REALIZADA Y LA EXPORTA A EXCEL
with conexion_wfm.connect().execution_options(autocommit=True) as conn:
    data0 = conn.execute(text(CONSULTA1))


Adherence = pd.DataFrame(data0.fetchall())
print("Data frame cargado")

Adherence.columns = Adherence.columns.str.upper()

# Convierte la columna FECHA a tipo datetime
Adherence['FECHA'] = pd.to_datetime(Adherence['FECHA'])

# Crea una columna "SEMANA" con valores predeterminados
Adherence['SEMANA'] = 0

Adherence.loc[(Adherence['FECHA'].dt.day >= 1) & (Adherence['FECHA'].dt.day <= 7), 'SEMANA'] = 1
Adherence.loc[(Adherence['FECHA'].dt.day >= 8) & (Adherence['FECHA'].dt.day <= 14), 'SEMANA'] = 2
Adherence.loc[(Adherence['FECHA'].dt.day >= 15) & (Adherence['FECHA'].dt.day <= 21), 'SEMANA'] = 3
Adherence.loc[Adherence['SEMANA'] == 0, 'SEMANA'] = 4

#AGREGA COLUMNA FECHA DE CARGUE
Adherence['FECHA_CARGUE']=pd.to_datetime('today')

name='bbdd_comisiones_retencion_inb_conv'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, ipADI, name))

Adherence.to_sql(con=database_connection, name='tbl_insumo_adh', if_exists='append', index=False, chunksize=1000)
print(Adherence.head)
print('Cargue base adherencia exitoso')



