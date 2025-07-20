import pandas as pd
import glob
import sqlalchemy
import datetime
from dateutil.relativedelta import relativedelta

exec(open(r'C:\Users\karol.orta\Pictures\Z_COPIAS_CARGUES3\conexion.py').read())
print("Entra")

name='bbdd_claro_retencion_convergente_modelo_02'

database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, 
                                                       ipADI, name))

ruta_archivos = r'Y:\3.COMISIONES\CLARO\3_RETENCION_CONVERGENTE\1_PROYECTO PYTHON_MODELO_2\0_BBDD_CARGUE\BASE_VISITAS\*.xlsx'
archivos = glob.glob(ruta_archivos)


tbl_BBDD = pd.DataFrame()
for archivo in archivos:

    tbl_BBDD_COS = pd.read_excel(archivo, sheet_name='Hoja1', skiprows=0)
    tbl_BBDD_COS = tbl_BBDD_COS.rename(columns=lambda x:x.strip())
    
    columnas_requeridas = ['USUARIO','CUENTA']

    tbl_BBDD_COS = tbl_BBDD_COS[columnas_requeridas]
    tbl_BBDD_COS.columns = tbl_BBDD_COS.columns.str.upper()
    
TB_HC = pd.read_sql_table('tbl_insumo_hc',con=database_connection)

documentos = tbl_BBDD.merge(TB_HC,how='inner',left_on='USUARIO', right_on='USUARIO_RR',indicator=True)
print(documentos.columns)

doc_map = documentos.drop_duplicates(subset='USUARIO', keep='first').set_index('USUARIO')['DOCUMENTO']
print(doc_map)

tbl_BBDD['DOCUMENTO'] = tbl_BBDD['USUARIO'].map(doc_map).fillna('')
doc_map = documentos.drop_duplicates(subset='USUARIO', keep='first').set_index('USUARIO')['NOMBRES_APELLIDOS']
print(doc_map)

tbl_BBDD['NOMBRES_APELLIDOS'] = tbl_BBDD['USUARIO'].map(doc_map).fillna('')
tbl_BBDD['MES']=(datetime.date.today() - relativedelta(months=1)).month

tbl_BBDD['AÑO']= (datetime.date.today() - relativedelta(months=1)).year

tbl_BBDD['FECHA_CARGUE'] = pd.to_datetime('today')

tbl_BBDD.to_sql(con=database_connection, name='tbl_insumo_visitas', if_exists='append', index=False, chunksize=1000)
print(tbl_BBDD.head)
print('CONECTADO')



