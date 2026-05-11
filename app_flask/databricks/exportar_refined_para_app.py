# Databricks notebook source
# MAGIC %md
# MAGIC # Exportar zona refined para la app Flask
# MAGIC
# MAGIC Este notebook exporta las carpetas Delta de refined a CSV planos con nombres limpios.

# COMMAND ----------

base_refined = "/Volumes/workspace/default/tcga_cancer_ml/refined"
export_path = f"{base_refined}/app_exports"
flat_path = f"{base_refined}/app_exports_flat"

datasets = {
    "refined_calidad_datos": f"{base_refined}/eda_outputs/calidad_datos",
    "refined_conteo_clases": f"{base_refined}/eda_outputs/conteo_clases",
    "refined_expresion_global": f"{base_refined}/eda_outputs/expresion_global",
    "refined_resumen_general": f"{base_refined}/eda_outputs/resumen_general",
    "refined_top_genes_por_clase": f"{base_refined}/eda_outputs/top_genes_por_clase",
    "refined_metricas_modelos_sparkml": f"{base_refined}/model_metrics/metricas_modelos_sparkml",
}

for nombre, ruta in datasets.items():
    print(f"Exportando {nombre} desde {ruta}")

    df = spark.read.format("delta").load(ruta)

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{export_path}/{nombre}")
    )

print("Exportación intermedia terminada en:", export_path)

dbutils.fs.mkdirs(flat_path)

for nombre in datasets:
    carpeta = f"{export_path}/{nombre}"

    archivos_csv = [
        f.path for f in dbutils.fs.ls(carpeta)
        if f.name.endswith(".csv")
    ]

    if not archivos_csv:
        print(f"No se encontró CSV para {nombre}")
        continue

    destino = f"{flat_path}/{nombre}.csv"
    dbutils.fs.cp(archivos_csv[0], destino)
    print(f"Copiado: {destino}")

display(dbutils.fs.ls(flat_path))
