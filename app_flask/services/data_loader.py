from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

TABLES = {
    "calidad_datos": "refined_calidad_datos",
    "conteo_clases": "refined_conteo_clases",
    "expresion_global": "refined_expresion_global",
    "resumen_general": "refined_resumen_general",
    "top_genes": "refined_top_genes_por_clase",
    "metricas": "refined_metricas_modelos_sparkml",
}


def get_data_mode() -> str:
    return os.getenv("DATA_MODE", "local").strip().lower()


def csv_path(table_name: str) -> Path:
    return DATA_DIR / f"{table_name}.csv"


def load_local_table(table_name: str) -> pd.DataFrame:
    path = csv_path(table_name)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def load_databricks_table(table_name: str, limit: int | None = None) -> pd.DataFrame:
    from databricks import sql

    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")
    catalog = os.getenv("DATABRICKS_CATALOG", "workspace")
    schema = os.getenv("DATABRICKS_SCHEMA", "default")

    missing = [
        name for name, value in {
            "DATABRICKS_SERVER_HOSTNAME": server_hostname,
            "DATABRICKS_HTTP_PATH": http_path,
            "DATABRICKS_TOKEN": access_token,
        }.items()
        if not value
    ]

    if missing:
        raise RuntimeError(f"Faltan variables de entorno para Databricks: {', '.join(missing)}")

    query = f"SELECT * FROM {catalog}.{schema}.{table_name}"
    if limit:
        query += f" LIMIT {int(limit)}"

    with sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(rows, columns=columns)


def load_table(key: str, limit: int | None = None) -> pd.DataFrame:
    if key not in TABLES:
        raise KeyError(f"Tabla no reconocida: {key}")

    table_name = TABLES[key]

    if get_data_mode() == "databricks":
        return load_databricks_table(table_name, limit=limit)

    return load_local_table(table_name)


def table_status() -> list[dict]:
    status = []
    mode = get_data_mode()

    for key, table_name in TABLES.items():
        if mode == "local":
            path = csv_path(table_name)
            exists = path.exists()
            rows = 0
            columns = 0

            if exists:
                try:
                    df = pd.read_csv(path)
                    rows = len(df)
                    columns = len(df.columns)
                except Exception:
                    rows = "Error al leer"
                    columns = "Error"

            status.append({
                "clave": key,
                "tabla": table_name,
                "modo": "local",
                "existe": exists,
                "filas": rows,
                "columnas": columns,
                "origen": str(path),
            })
        else:
            try:
                df = load_databricks_table(table_name, limit=5)
                status.append({
                    "clave": key,
                    "tabla": table_name,
                    "modo": "databricks",
                    "existe": True,
                    "filas": "Consulta OK",
                    "columnas": len(df.columns),
                    "origen": f"{os.getenv('DATABRICKS_CATALOG', 'workspace')}.{os.getenv('DATABRICKS_SCHEMA', 'default')}.{table_name}",
                })
            except Exception as exc:
                status.append({
                    "clave": key,
                    "tabla": table_name,
                    "modo": "databricks",
                    "existe": False,
                    "filas": "Error",
                    "columnas": 0,
                    "origen": str(exc),
                })

    return status


def load_all() -> dict[str, pd.DataFrame]:
    return {key: load_table(key) for key in TABLES}
