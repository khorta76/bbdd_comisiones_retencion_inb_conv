import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')
exec(open(r'Y:\3.COMISIONES\Z_EQUIPO_COMISIONES\02_Karol_Horta\Z\conexion.py').read())

mes = (datetime.now() - relativedelta(months=1)).month
anho = (datetime.now() - relativedelta(months=1)).year


POLITICAS = """
    SELECT * FROM x_informe_comisiones.tbl_politicas_comerciales_v2
    WHERE ID_CENTRO_COSTO = '7401'
	    AND FECHA_FIN IS NULL;
"""

tbl_politicas = pd.read_sql(POLITICAS, conexion_adi.connect())
# Función para encontrar la tarifa correcta para cada asesor según el tipo facturable
def obtener_tarifa(tipo_facturable, cargo, segmento = '', kpi_1=0,kpi_2=0, kpi_3=0, kpi_4 = 0):
    # Filtramos tbl_politicas según el tipo facturable que se pase como parámetro
    politicas_filtradas = tbl_politicas[(tbl_politicas['TIPO_FACTURABLE'] == tipo_facturable) & (tbl_politicas['CARGO'] == cargo) & (tbl_politicas['SERVICIO'] == segmento)]
    
    politicas_filtradas[['INDICADOR_1', 'INDICADOR_2', 'INDICADOR_3', 'INDICADOR_4', 'INDICADOR_5', 'INDICADOR_6', 'INDICADOR_7', 'INDICADOR_8']] = politicas_filtradas[['INDICADOR_1', 'INDICADOR_2', 'INDICADOR_3', 'INDICADOR_4', 'INDICADOR_5', 'INDICADOR_6', 'INDICADOR_7', 'INDICADOR_8']].fillna(0)

    # Aplicamos el filtro de indicadores
    filtro = (
    (kpi_1 >= politicas_filtradas['INDICADOR_1']) & (kpi_1 <= politicas_filtradas['INDICADOR_2']) &
    (kpi_2 >= politicas_filtradas['INDICADOR_3']) & (kpi_2 <= politicas_filtradas['INDICADOR_4']) &
    (kpi_3 >= politicas_filtradas['INDICADOR_5']) & (kpi_3 <= politicas_filtradas['INDICADOR_6']) &
    (kpi_4 >= politicas_filtradas['INDICADOR_7']) & (kpi_4 <= politicas_filtradas['INDICADOR_8'])
)

    
    # Obtenemos la tarifa correspondiente
    tarifa = politicas_filtradas.loc[filtro, 'TARIFA']
    
    warnings.filterwarnings(
    "ignore",
    message=".*Downcasting object dtype arrays on .fillna.*",
    category=FutureWarning)


    return tarifa.values[0] if not tarifa.empty else 0  # 0 si no encuentra coincidencia



CALENDARIO_DIARIO = f"""
    SELECT * FROM adi_complementos.calendario_diario
    WHERE MES2 = {mes}
	    AND AÑO = {anho};
"""

tbl_calendario_diario = pd.read_sql(CALENDARIO_DIARIO, conexion_adi.connect())



INFORMACION_CAMPANA = """
  SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_campana;
"""

tbl_campana = pd.read_sql(INFORMACION_CAMPANA, conexion_adi.connect())

tbl_campana = tbl_campana.rename(columns={'id': 'CAMPANA_ID'})

tbl_campana.columns = tbl_campana.columns.str.upper()


DISTRIBUCIONHC = f"""
    SELECT 
MES,
ANHO, 
`KEY`, 
DOCUMENTO, 
NOMBRE, CARGO, 
SEGMENTO, 
NOMBRE_SUPERVISOR, 
FECHA_INGRESO, 
ESTADO, 
NOMBRE_JEFE_DE_OPERACION, 
ANTIGUEDAD, 
USUARIO_RR, 
TIPO_DE_DOCUMENTO
FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_hc
WHERE SEGMENTO != 'Polivalentes'
AND ESTADO != 'Retiro'
AND MES = {mes}
      AND ANHO = {anho};
"""

tbl_insumo_hc = pd.read_sql(DISTRIBUCIONHC, conexion_adi.connect())


CONSULTA1 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_adh
   WHERE MES = {mes}
	    AND ANHO = {anho};'''

tbl_insumo_adh = pd.read_sql(CONSULTA1, conexion_adi.connect())
print('tbl_insumo_adh OK')

CONSULTA2 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tb_insumo_efectividad_conv
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tb_insumo_efectividad_conv = pd.read_sql(CONSULTA2, conexion_adi.connect())
print('tb_insumo_efectividad_conv OK')


CONSULTA3 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tb_insumo_efectividad_cav
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tb_insumo_efectividad_cav = pd.read_sql(CONSULTA3, conexion_adi.connect())
print('tb_insumo_efectividad_cav OK')

CONSULTA4 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tb_insumo_efectividad_adic
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tb_insumo_efectividad_adic = pd.read_sql(CONSULTA4, conexion_adi.connect())
print('tb_insumo_efectividad_adic OK')

CONSULTA5 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_gtc
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tb_insumo_efectividad_gtc = pd.read_sql(CONSULTA5, conexion_adi.connect())
print('tb_insumo_efectividad_gtc OK')

CONSULTA6 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_ctarifas
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tbl_insumo_ctarifas = pd.read_sql(CONSULTA6, conexion_adi.connect())
print('tbl_insumo_ctarifas OK')


CONSULTA7 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_colocaciones
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tbl_insumo_colocaciones = pd.read_sql(CONSULTA7, conexion_adi.connect())
print('tbl_insumo_colocaciones OK')

CONSULTA8 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_celula_adic
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tbl_insumo_celula_adic = pd.read_sql(CONSULTA8, conexion_adi.connect())
print('tbl_insumo_celula_adic OK')

CONSULTA9 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_calidad_conv
 WHERE MES = {mes}
    AND ANHO = {anho};'''

tbl_insumo_calidad_conv = pd.read_sql(CONSULTA9, conexion_adi.connect())
print('tbl_insumo_calidad_conv OK')

CONSULTA10 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_calidad_adic
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tbl_insumo_calidad_adic = pd.read_sql(CONSULTA10, conexion_adi.connect())
print('tbl_insumo_calidad_adic OK')

CONSULTA11 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_bmovil
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tbl_insumo_bmovil = pd.read_sql(CONSULTA11, conexion_adi.connect())
print('tbl_insumo_bmovil OK')

CONSULTA12 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_bhogar
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tbl_insumo_bhogar = pd.read_sql(CONSULTA12, conexion_adi.connect())
print('tbl_insumo_bhogar OK')

CONSULTA13 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_ventas_adic
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tbl_insumo_ventas_adic = pd.read_sql(CONSULTA13, conexion_adi.connect())
print('tbl_insumo_ventas_adic OK')


CONSULTA15 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_retenidos_adic
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tbl_insumo_retenidos_adic = pd.read_sql(CONSULTA15, conexion_adi.connect())
print('tbl_insumo_retenidos_adic OK')

CONSULTA16 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_renegociaciones_descto
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tbl_insumo_renegociaciones_descto = pd.read_sql(CONSULTA16, conexion_adi.connect())
print('tbl_insumo_renegociaciones_descto OK')

CONSULTA17 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_renegociaciones_gestores
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tbl_insumo_renegociaciones_gestores = pd.read_sql(CONSULTA17, conexion_adi.connect())
print('tbl_insumo_renegociaciones_gestores OK')

CONSULTA18 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_marcacion_soul
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tbl_insumo_marcacion_soul = pd.read_sql(CONSULTA18, conexion_adi.connect())
print('tbl_insumo_marcacion_soul OK')

CONSULTA19 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_llamadas_outhb
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tbl_insumo_llamadas_outhb = pd.read_sql(CONSULTA19, conexion_adi.connect())
print('tbl_insumo_llamadas_outhb OK')

CONSULTA20 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tbl_insumo_inf_op
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tbl_insumo_inf_op = pd.read_sql(CONSULTA20, conexion_adi.connect())
print('tbl_insumo_inf_op OK')


CONSULTA21 = f'''SELECT * FROM bbdd_comisiones_retencion_inb_conv.tb_per_asistencia
 WHERE MES = {mes}
	    AND ANHO = {anho};'''

tb_per_asistencia = pd.read_sql(CONSULTA21, conexion_adi.connect())
print('tb_per_asistencia OK')