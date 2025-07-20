import pandas as pd
import glob
import sqlalchemy

exec(open(r'C:\Users\karol.orta\Pictures\Z_COPIAS_CARGUES3\conexion.py').read())


ruta_archivos = r'Y:\3.COMISIONES\CLARO\3_RETENCION_CONVERGENTE\1_PROYECTO PYTHON_MODELO_2\0_BBDD_CARGUE\LDS MAL TRANSFERIDAS\*.xlsx'
archivos = glob.glob(ruta_archivos)

tbl_BBDD = pd.DataFrame()
for archivo in archivos:
    tbl_BBDD_COS = pd.read_excel(archivo, sheet_name='Hoja2', skiprows=0)
    tbl_BBDD_COS = tbl_BBDD_COS.rename(columns=lambda x:x.strip())
    
    print(archivo)

    columnas_requeridas = ['id_registro','fecha_creacion','Documento','Asesor']

    tbl_BBDD_COS = tbl_BBDD_COS[columnas_requeridas]
    tbl_BBDD_COS.columns = tbl_BBDD_COS.columns.str.upper()

tbl_BBDD_COS = tbl_BBDD_COS.rename(columns={'FECHA_CREACION':'FECHA'}) 
tbl_BBDD_COS['FECHA'] = pd.to_datetime(tbl_BBDD_COS['FECHA'])


tbl_BBDD['MES'] = pd.to_datetime(tbl_BBDD['FECHA']).dt.month

tbl_BBDD['AÑO'] = pd.to_datetime(tbl_BBDD['FECHA']).dt.year
tbl_BBDD['FECHA'] = (tbl_BBDD_COS['FECHA']).dt.date
tbl_BBDD['FECHA_CARGUE'] = pd.to_datetime('today')

name='bbdd_claro_retencion_convergente_modelo_02'

database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, 
                                                       ipADI, name))

tbl_BBDD.to_sql(con=database_connection, name='tbl_insumo_lds_mal_transf', if_exists='append', index=False, chunksize=1000)
print(tbl_BBDD.head)
print('conectado')




