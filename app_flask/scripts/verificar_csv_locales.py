from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / "data"

expected = [
    "refined_calidad_datos.csv",
    "refined_conteo_clases.csv",
    "refined_expresion_global.csv",
    "refined_resumen_general.csv",
    "refined_top_genes_por_clase.csv",
    "refined_metricas_modelos_sparkml.csv",
]

for filename in expected:
    path = DATA_DIR / filename
    if not path.exists():
        print(f"NO ENCONTRADO: {filename}")
        continue

    df = pd.read_csv(path)
    print(f"OK: {filename} | filas={len(df)} | columnas={len(df.columns)}")
    print("Columnas:", list(df.columns))
    print("-" * 80)
