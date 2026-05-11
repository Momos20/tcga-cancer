# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %run /tcga_cancer_batch_databricks/00_configuracion

# COMMAND ----------

import os
import json
import time
import requests
import pandas as pd
from pathlib import Path
from pyspark.sql import functions as F

FILES_ENDPT = "https://api.gdc.cancer.gov/files"
DATA_ENDPT = "https://api.gdc.cancer.gov/data"

print("Ruta raw RNA-Seq:", RAW_RNASEQ_PATH)
print("Ruta raw metadata:", RAW_METADATA_PATH)

# COMMAND ----------

# Consultar archivos RNA-Seq STAR Counts en GDC

filters = {
    "op": "and",
    "content": [
        {
            "op": "in",
            "content": {
                "field": "cases.project.project_id",
                "value": PROYECTOS_PRINCIPALES
            }
        },
        {
            "op": "in",
            "content": {
                "field": "data_category",
                "value": ["Transcriptome Profiling"]
            }
        },
        {
            "op": "in",
            "content": {
                "field": "data_type",
                "value": ["Gene Expression Quantification"]
            }
        },
        {
            "op": "in",
            "content": {
                "field": "analysis.workflow_type",
                "value": ["STAR - Counts"]
            }
        },
        {
            "op": "in",
            "content": {
                "field": "access",
                "value": ["open"]
            }
        }
    ]
}

fields = [
    "file_id",
    "file_name",
    "file_size",
    "cases.case_id",
    "cases.submitter_id",
    "cases.project.project_id",
    "cases.samples.sample_id",
    "cases.samples.submitter_id",
    "cases.samples.sample_type"
]

params = {
    "filters": json.dumps(filters),
    "fields": ",".join(fields),
    "format": "JSON",
    "size": "20000"
}

response = requests.get(FILES_ENDPT, params=params)
response.raise_for_status()

data = response.json()
archivos = data["data"]["hits"]

print("Archivos encontrados en GDC:", len(archivos))

# COMMAND ----------

# Crear tabla de metadatos desde respuesta de GDC

registros = []

for archivo in archivos:
    file_id = archivo.get("file_id")
    file_name = archivo.get("file_name")
    file_size = archivo.get("file_size")

    cases = archivo.get("cases", [])

    if len(cases) == 0:
        continue

    caso = cases[0]

    case_id = caso.get("case_id")
    case_submitter_id = caso.get("submitter_id")

    project = caso.get("project", {})
    project_id = project.get("project_id")

    cancer_type = mapa_cancer.get(project_id, "DESCONOCIDO")
    cancer_name = mapa_nombre_cancer.get(project_id, "DESCONOCIDO")

    samples = caso.get("samples", [])

    if len(samples) > 0:
        sample_id = samples[0].get("sample_id")
        sample_submitter_id = samples[0].get("submitter_id")
        sample_type = samples[0].get("sample_type")
    else:
        sample_id = None
        sample_submitter_id = None
        sample_type = None

    registros.append({
        "file_id": file_id,
        "file_name": file_name,
        "file_size": file_size,
        "case_id": case_id,
        "case_submitter_id": case_submitter_id,
        "sample_id": sample_id,
        "sample_submitter_id": sample_submitter_id,
        "sample_type": sample_type,
        "project_id": project_id,
        "cancer_type": cancer_type,
        "cancer_name": cancer_name
    })

df_metadatos = pd.DataFrame(registros)

print("Dimensiones de metadatos:")
print(df_metadatos.shape)

display(df_metadatos.head())

print("Conteo por tipo de cáncer:")
display(
    df_metadatos["cancer_type"]
    .value_counts()
    .reset_index()
    .rename(columns={"index": "cancer_type", "cancer_type": "n_archivos"})
)

# COMMAND ----------

# Filtrar dataset oficial: 18 clases + Primary Tumor

df_metadatos_oficial = df_metadatos[
    df_metadatos["cancer_type"].isin(CLASES_PRINCIPALES)
].copy()

df_metadatos_oficial = df_metadatos_oficial[
    df_metadatos_oficial["sample_type"]
    .astype(str)
    .str.contains("Primary Tumor", case=False, na=False)
].copy()

print("Archivos oficiales después de filtrar 18 clases + Primary Tumor:")
print(df_metadatos_oficial.shape)

display(
    df_metadatos_oficial["cancer_type"]
    .value_counts()
    .reset_index()
    .rename(columns={"index": "cancer_type", "cancer_type": "n_archivos"})
)

# COMMAND ----------

# Guardar metadatos en zona raw/metadata
metadata_completo_path = f"{RAW_METADATA_PATH}/metadatos_tcga_completo.csv"
metadata_oficial_path = f"{RAW_METADATA_PATH}/metadatos_tcga_oficial_18_clases.csv"

df_metadatos.to_csv(metadata_completo_path, index=False)
df_metadatos_oficial.to_csv(metadata_oficial_path, index=False)

print("Metadatos guardados en:")
print(metadata_completo_path)
print(metadata_oficial_path)

# COMMAND ----------

# Catalogar metadatos como tablas Delta

spark_metadatos = spark.createDataFrame(df_metadatos)
spark_metadatos_oficial = spark.createDataFrame(df_metadatos_oficial)

(
    spark_metadatos
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.raw_tcga_metadatos_completo")
)

(
    spark_metadatos_oficial
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.raw_tcga_metadatos_oficial_18_clases")
)

# COMMAND ----------

# Descargar archivos RNA-Seq desde GDC hacia raw/rnaseq

def descargar_archivo_gdc(file_id, file_name, carpeta_destino, max_reintentos=3):
    """
    Descarga un archivo individual desde GDC usando file_id.
    Guarda cada archivo dentro de una carpeta con su file_id.
    """

    carpeta_file_id = Path(carpeta_destino) / file_id
    carpeta_file_id.mkdir(parents=True, exist_ok=True)

    ruta_salida = carpeta_file_id / file_name

    if ruta_salida.exists() and ruta_salida.stat().st_size > 0:
        return "ya_existe", str(ruta_salida)

    url = f"{DATA_ENDPT}/{file_id}"

    for intento in range(1, max_reintentos + 1):
        try:
            with requests.get(url, stream=True, timeout=180) as r:
                r.raise_for_status()

                with open(ruta_salida, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

            return "descargado", str(ruta_salida)

        except Exception as e:
            print(f"Error descargando {file_id} intento {intento}: {e}")
            time.sleep(5)

    return "error", str(ruta_salida)


# Para descargar todo, dejar en None.
MAX_DESCARGAS = None

df_descarga = df_metadatos_oficial.copy()

if MAX_DESCARGAS is not None:
    df_descarga = df_descarga.head(MAX_DESCARGAS)

print("Archivos a descargar:", len(df_descarga))

registros_descarga = []

for idx, fila in df_descarga.iterrows():
    file_id = fila["file_id"]
    file_name = fila["file_name"]

    estado, ruta_local = descargar_archivo_gdc(
        file_id=file_id,
        file_name=file_name,
        carpeta_destino=RAW_RNASEQ_PATH
    )

    registros_descarga.append({
        "file_id": file_id,
        "file_name": file_name,
        "estado_descarga": estado,
        "ruta_local": ruta_local
    })

    if len(registros_descarga) % 50 == 0:
        print(f"Procesados {len(registros_descarga)} de {len(df_descarga)}")

df_descargas = pd.DataFrame(registros_descarga)

print("Resumen de descargas:")
display(df_descargas["estado_descarga"].value_counts().reset_index())

display(df_descargas.head())

# COMMAND ----------

# Guardar manifiesto de descargas

manifest_descargas_path = f"{RAW_METADATA_PATH}/manifest_descargas_gdc.csv"

df_descargas.to_csv(manifest_descargas_path, index=False)

spark_descargas = spark.createDataFrame(df_descargas)

(
    spark_descargas
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.raw_tcga_manifest_descargas")
)

print("Manifiesto de descargas guardado en:")
print(manifest_descargas_path)

print("Tabla Delta creada:")
print("workspace.default.raw_tcga_manifest_descargas")

# COMMAND ----------

# Validar archivos descargados en raw/rnaseq
archivos_descargados = []

for root, dirs, files in os.walk(RAW_RNASEQ_PATH):
    for file in files:
        if file.endswith(".tsv") and "rna_seq.augmented_star_gene_counts" in file.lower():
            archivos_descargados.append(os.path.join(root, file))

print("Archivos RNA-Seq descargados encontrados:", len(archivos_descargados))

print("Primeros archivos:")
for archivo in archivos_descargados[:10]:
    print(archivo)