import subprocess
import os

# Ruta base donde están tus scripts (ajustada a tu estructura)
base_path = os.path.dirname(os.path.abspath(__file__))

# Lista de scripts a ejecutar (con rutas completas)
scripts = [
    'libreria.py',
    'detalle_asesores.py',
    'detalle_supervisores.py',
    'resumen.py',
    'exportador.py'
]

for script in scripts:
    script_path = os.path.join(base_path, script)
    print(f"Ejecutando: {script_path}")
    subprocess.run(['C:/Phyton36/python.exe', script_path], check=True)

print('Ejecución finalizada con éxito')
