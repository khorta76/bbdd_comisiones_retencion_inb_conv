from sqlalchemy import text
import import_ipynb
import nbimporter
from detalle_asesores import detalle_asesores, detalle_asesores_semana, detalle_asesores_mensual
from detalle_supervisores import detalle_supervisor
from resumen import resumen, informe_re
from libreria import mes, anho
import warnings

warnings.filterwarnings('ignore')

exec(open(r'Y:\3.COMISIONES\Z_EQUIPO_COMISIONES\01_jailer\01_conexion.py').read())

detalle_asesores = detalle_asesores.drop(columns=['ID'])
detalle_asesores_semana = detalle_asesores_semana.drop(columns=['ID'])
detalle_asesores_mensual = detalle_asesores_mensual.drop(columns=['ID'])
detalle_supervisores = detalle_supervisor.drop(columns=['ID'])

tablas_a_limpiar = [
        'tbl_detalle_asesores',
        'tbl_detalle_asesores_semana',
        'tbl_detalle_asesores_mensual',
        'tbl_detalle_supervisores',
        'tbl_resumen',
        'tbl_informe_re'
    ]

for i in tablas_a_limpiar:

    delete_sql = f"""
        DELETE FROM bbdd_comisiones_whatsapp_convergente.{i}
        WHERE MES = {mes}
        AND ANHO = {anho};"""
    

    with conexion_adi.begin() as conn:
        conn.execute(text(delete_sql))
    print(f'Delete {i} mes {mes}, año {anho} realizado con exito')

name='bbdd_comisiones_whatsapp_convergente'

database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                  format(userADI, passwordADI, 
                                                       ipADI, name))


detalle_asesores.to_sql(con=database_connection, name='tbl_detalle_asesores', if_exists='append', index=False, chunksize=1000)
print(f'cargue detalle_asesores mes {mes}, año {anho} realizado con exito')

detalle_asesores_semana.to_sql(con=database_connection, name='tbl_detalle_asesores_semana', if_exists='append', index=False, chunksize=1000)
print(f'cargue detalle_asesores_semana mes {mes}, año {anho} realizado con exito')

detalle_asesores_mensual.to_sql(con=database_connection, name='tbl_detalle_asesores_mensual', if_exists='append', index=False, chunksize=1000)
print(f'cargue detalle_asesores_mensual mes {mes}, año {anho} realizado con exito')

detalle_supervisores.to_sql(con=database_connection, name='tbl_detalle_supervisores', if_exists='append', index=False, chunksize=1000)
print(f'cargue detalle_supervisores mes {mes}, año {anho} realizado con exito')

resumen.to_sql(con=database_connection, name='tbl_resumen', if_exists='append', index=False, chunksize=1000)
print(f'cargue resumen mes {mes}, año {anho} realizado con exito')

informe_re.to_sql(con=database_connection, name='tbl_informe_re', if_exists='append', index=False, chunksize=1000)
print(f'cargue informe_re mes {mes}, año {anho} realizado con exito')


print('Cargues realizados')


