import pandas as pd
import nbimporter
from libreria import obtener_tarifa, tbl_calendario_diario,tbl_insumo_hc, tbl_insumo_adh, tbl_insumo_bhogar  ,tbl_insumo_bmovil,tbl_insumo_renegociaciones_gestores,tbl_insumo_marcacion_soul,tbl_insumo_llamadas_outhb,tbl_insumo_inf_op,tb_per_asistencia
import warnings
#from tabulate import tabulate


tbl_insumo_hc = tbl_insumo_hc[tbl_insumo_hc['SEGMENTO'].isin(['Backoffice', 'Renegociacion'])]

tbl_asesores_renegociacion = tbl_insumo_hc.merge(
        tbl_insumo_adh,
        on=['DOCUMENTO','MES','ANHO'],
        how='left',
)

tbl_asesores_renegociacion = tbl_asesores_renegociacion.merge(
    tbl_insumo_renegociaciones_gestores,
    left_on=['DOCUMENTO', 'FECHA', 'MES', 'ANHO'],
    right_on=['DOCUMENTO_ASESOR', 'FECHA_DE_CREACION', 'MES', 'ANHO'],
    how='left'
)


print(tbl_asesores_renegociacion.head(5))
#print(tabulate(tbl_asesores_renegociacion, headers='keys', tablefmt='fancy_grid'))