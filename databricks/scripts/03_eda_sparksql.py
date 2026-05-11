# Databricks notebook source
# MAGIC %run ./00_configuracion

# COMMAND ----------

# Análisis exploratorio de datos usando SparkSQL

from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import pandas as pd

# Asegurar rutas refined
dbutils.fs.mkdirs(REFINED_EDA_PATH)
dbutils.fs.mkdirs(REFINED_VISUALIZATIONS_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Carga de tablas trusted catalogadas

# COMMAND ----------

TABLA_LONG = "workspace.default.trusted_tcga_rnaseq_long_18_clases"
TABLA_SAMPLES = "workspace.default.trusted_tcga_samples_18_clases"
TABLA_GENES = "workspace.default.trusted_tcga_gene_dictionary"

df_trusted = spark.table(TABLA_LONG)
df_samples = spark.table(TABLA_SAMPLES)
df_genes = spark.table(TABLA_GENES)

print("\nRegistros long:", df_trusted.count())
print("Filas samples:", df_samples.count())
print("Genes en diccionario:", df_genes.count())

display(df_trusted.limit(5))

# COMMAND ----------

# DBTITLE 1,Función auxiliar para guardar resultados en refined
# Función auxiliar para guardar resultados en refined

def guardar_refined(df, nombre_tabla, subcarpeta):
    '''
    Guarda un DataFrame en la zona refined/eda_outputs y lo cataloga como tabla Delta.
    '''
    # Eliminar tabla si existe
    spark.sql(f"DROP TABLE IF EXISTS workspace.default.{nombre_tabla}")
    
    # Guardar directamente como tabla Unity Catalog
    (
        df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"workspace.default.{nombre_tabla}")
    )

    print(f"Tabla creada: workspace.default.{nombre_tabla}")
    
    # Retornar la ruta teórica (aunque Unity Catalog maneja la ubicación automáticamente)
    ruta = f"{REFINED_EDA_PATH}/{subcarpeta}"
    return ruta

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Validación general de consistencia

# COMMAND ----------

# DBTITLE 1,Resumen general de consistencia
# Resumen general de consistencia
eda_resumen_general = spark.sql(f"""
    SELECT
        (SELECT COUNT(*) FROM {TABLA_LONG}) AS n_registros_long,
        (SELECT COUNT(DISTINCT sample_id) FROM {TABLA_LONG}) AS n_muestras_long,
        (SELECT COUNT(*) FROM {TABLA_SAMPLES}) AS n_filas_samples,
        (SELECT COUNT(DISTINCT sample_id) FROM {TABLA_SAMPLES}) AS n_muestras_samples,
        (SELECT COUNT(DISTINCT patient_id) FROM {TABLA_LONG}) AS n_pacientes,
        (SELECT COUNT(DISTINCT cancer_type) FROM {TABLA_LONG}) AS n_clases,
        (SELECT COUNT(DISTINCT gene_id_base) FROM {TABLA_LONG}) AS n_genes_long,
        (SELECT COUNT(DISTINCT gene_id_base) FROM {TABLA_GENES}) AS n_genes_diccionario
""")

display(eda_resumen_general)

# Guardar directamente sin usar la función que tiene el error
spark.sql("DROP TABLE IF EXISTS workspace.default.refined_eda_resumen_general")

(
    eda_resumen_general
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.refined_eda_resumen_general")
)

print("Tabla creada: workspace.default.refined_eda_resumen_general")

# COMMAND ----------

# Validaciones de duplicados
duplicados_samples = (
    df_samples
    .groupBy("sample_id")
    .agg(F.count("*").alias("n_filas"))
    .filter(F.col("n_filas") > 1)
)

duplicados_sample_gene = (
    df_trusted
    .groupBy("sample_id", "gene_id_base")
    .agg(F.count("*").alias("n_filas"))
    .filter(F.col("n_filas") > 1)
)

print("Duplicados por sample_id en tabla samples:", duplicados_samples.count())
print("Duplicados por sample_id + gene_id_base en tabla long:", duplicados_sample_gene.count())

display(duplicados_samples.limit(10))
display(duplicados_sample_gene.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Distribución de clases

# COMMAND ----------

# Conteo de muestras y pacientes por tipo de cáncer
eda_conteo_clases = spark.sql(f"""
    SELECT
        cancer_type,
        cancer_name,
        COUNT(DISTINCT sample_id) AS n_muestras,
        COUNT(DISTINCT patient_id) AS n_pacientes
    FROM {TABLA_LONG}
    GROUP BY cancer_type, cancer_name
    ORDER BY n_muestras DESC
""")

display(eda_conteo_clases)

guardar_refined(
    eda_conteo_clases,
    "refined_eda_conteo_clases",
    "conteo_clases"
)

# COMMAND ----------

# Análisis formal de desbalance de clases
eda_desbalance_clases = spark.sql(f"""
    WITH conteo AS (
        SELECT
            cancer_type,
            COUNT(DISTINCT sample_id) AS n_muestras
        FROM {TABLA_LONG}
        GROUP BY cancer_type
    ),
    total AS (
        SELECT SUM(n_muestras) AS total_muestras FROM conteo
    ),
    extremos AS (
        SELECT
            MAX(n_muestras) AS max_muestras,
            MIN(n_muestras) AS min_muestras
        FROM conteo
    )
    SELECT
        c.cancer_type,
        c.n_muestras,
        ROUND(100 * c.n_muestras / t.total_muestras, 2) AS porcentaje,
        ROUND(e.max_muestras / e.min_muestras, 2) AS razon_desbalance_global
    FROM conteo c
    CROSS JOIN total t
    CROSS JOIN extremos e
    ORDER BY c.n_muestras DESC
""")

display(eda_desbalance_clases)

guardar_refined(
    eda_desbalance_clases,
    "refined_eda_desbalance_clases",
    "desbalance_clases"
)

# COMMAND ----------

# Gráfico de distribución de clases

pdf_clases = eda_conteo_clases.toPandas()

plt.figure(figsize=(13, 6))
plt.bar(pdf_clases["cancer_type"], pdf_clases["n_muestras"])
plt.title("Distribución de muestras por tipo de cáncer")
plt.xlabel("Tipo de cáncer")
plt.ylabel("Número de muestras")
plt.xticks(rotation=45)
plt.tight_layout()

ruta_grafico = f"{REFINED_VISUALIZATIONS_PATH}/distribucion_clases.png"
plt.savefig(ruta_grafico, dpi=300, bbox_inches="tight")
plt.show()

print("Gráfico guardado en:", ruta_grafico)

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Validación de tipos de muestra y alcance del dataset

# COMMAND ----------

# Validar tipos de muestra
eda_sample_type = spark.sql(f"""
    SELECT
        sample_type,
        COUNT(DISTINCT sample_id) AS n_muestras,
        COUNT(DISTINCT patient_id) AS n_pacientes
    FROM {TABLA_LONG}
    GROUP BY sample_type
    ORDER BY n_muestras DESC
""")

display(eda_sample_type)

guardar_refined(
    eda_sample_type,
    "refined_eda_tipos_muestra",
    "tipos_muestra"
)

# COMMAND ----------

# Validar clases oficiales

clases_obtenidas = set([r["cancer_type"] for r in df_samples.select("cancer_type").distinct().collect()])
clases_esperadas = set(CLASES_PRINCIPALES)

print("Número de clases esperadas:", len(clases_esperadas))
print("Número de clases obtenidas:", len(clases_obtenidas))
print("Clases faltantes:", clases_esperadas - clases_obtenidas)
print("Clases no esperadas:", clases_obtenidas - clases_esperadas)

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Calidad de datos en la expresión génica

# COMMAND ----------

# Calidad de datos: nulos y ceros

eda_calidad_datos = spark.sql(f"""
    SELECT
        cancer_type,
        COUNT(*) AS n_registros,
        SUM(CASE WHEN log2_tpm IS NULL THEN 1 ELSE 0 END) AS n_log2_tpm_null,
        SUM(CASE WHEN tpm_unstranded IS NULL THEN 1 ELSE 0 END) AS n_tpm_null,
        SUM(CASE WHEN tpm_unstranded = 0 THEN 1 ELSE 0 END) AS n_tpm_cero,
        ROUND(100 * SUM(CASE WHEN tpm_unstranded = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_tpm_cero
    FROM {TABLA_LONG}
    GROUP BY cancer_type
    ORDER BY pct_tpm_cero DESC
""")

display(eda_calidad_datos)

guardar_refined(
    eda_calidad_datos,
    "refined_eda_calidad_datos",
    "calidad_datos"
)

# COMMAND ----------

# Genes detectados por muestra
# Un gen se considera detectado si TPM > 0

eda_genes_detectados_muestra = spark.sql(f"""
    WITH genes_por_muestra AS (
        SELECT
            sample_id,
            cancer_type,
            SUM(CASE WHEN tpm_unstranded > 0 THEN 1 ELSE 0 END) AS genes_detectados,
            COUNT(*) AS genes_totales
        FROM {TABLA_LONG}
        GROUP BY sample_id, cancer_type
    )
    SELECT
        cancer_type,
        COUNT(*) AS n_muestras,
        ROUND(AVG(genes_detectados), 2) AS media_genes_detectados,
        ROUND(STDDEV(genes_detectados), 2) AS sd_genes_detectados,
        MIN(genes_detectados) AS min_genes_detectados,
        MAX(genes_detectados) AS max_genes_detectados,
        ROUND(100 * AVG(genes_detectados / genes_totales), 2) AS pct_promedio_genes_detectados
    FROM genes_por_muestra
    GROUP BY cancer_type
    ORDER BY media_genes_detectados DESC
""")

display(eda_genes_detectados_muestra)

guardar_refined(
    eda_genes_detectados_muestra,
    "refined_eda_genes_detectados_muestra",
    "genes_detectados_muestra"
)

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Estadísticas de expresión génica

# COMMAND ----------

# Estadísticas globales de expresión por clase

eda_expresion_global = spark.sql(f"""
    SELECT
        cancer_type,
        COUNT(*) AS n_registros,
        ROUND(AVG(log2_tpm), 4) AS media_log2_tpm,
        ROUND(STDDEV(log2_tpm), 4) AS sd_log2_tpm,
        ROUND(MIN(log2_tpm), 4) AS min_log2_tpm,
        ROUND(percentile_approx(log2_tpm, 0.25), 4) AS q1_log2_tpm,
        ROUND(percentile_approx(log2_tpm, 0.50), 4) AS mediana_log2_tpm,
        ROUND(percentile_approx(log2_tpm, 0.75), 4) AS q3_log2_tpm,
        ROUND(MAX(log2_tpm), 4) AS max_log2_tpm
    FROM {TABLA_LONG}
    GROUP BY cancer_type
    ORDER BY cancer_type
""")

display(eda_expresion_global)

guardar_refined(
    eda_expresion_global,
    "refined_eda_expresion_global",
    "expresion_global"
)

# COMMAND ----------

# Gráfico de expresión promedio por clase

pdf_expr = eda_expresion_global.toPandas().sort_values("media_log2_tpm", ascending=False)

plt.figure(figsize=(13, 6))
plt.bar(pdf_expr["cancer_type"], pdf_expr["media_log2_tpm"])
plt.title("Expresión promedio log2(TPM + 1) por tipo de cáncer")
plt.xlabel("Tipo de cáncer")
plt.ylabel("Media log2(TPM + 1)")
plt.xticks(rotation=45)
plt.tight_layout()

ruta_grafico_expr = f"{REFINED_VISUALIZATIONS_PATH}/expresion_promedio_por_clase.png"
plt.savefig(ruta_grafico_expr, dpi=300, bbox_inches="tight")
plt.show()

print("Gráfico guardado en:", ruta_grafico_expr)

# COMMAND ----------

# MAGIC %md
# MAGIC 7. Genes más expresados por clase
# MAGIC

# COMMAND ----------

# Top 10 genes más expresados por clase
eda_top_genes_por_clase = spark.sql(f"""
    WITH expresion_gen AS (
        SELECT
            cancer_type,
            gene_id_base,
            gene_name,
            AVG(log2_tpm) AS avg_log2_tpm
        FROM {TABLA_LONG}
        GROUP BY cancer_type, gene_id_base, gene_name
    ),
    ranking AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY cancer_type
                ORDER BY avg_log2_tpm DESC
            ) AS rank_gen
        FROM expresion_gen
    )
    SELECT
        cancer_type,
        rank_gen,
        gene_id_base,
        gene_name,
        ROUND(avg_log2_tpm, 4) AS avg_log2_tpm
    FROM ranking
    WHERE rank_gen <= 10
    ORDER BY cancer_type, rank_gen
""")

display(eda_top_genes_por_clase)

guardar_refined(
    eda_top_genes_por_clase,
    "refined_eda_top_genes_por_clase",
    "top_genes_por_clase"
)

# COMMAND ----------

# MAGIC %md
# MAGIC 8. Genes con mayor variabilidad entre clases

# COMMAND ----------

# Genes con mayor variabilidad entre tipos de cáncer

TOP_GENES_VARIABLES = 5000
eda_genes_mas_variables = spark.sql(f"""
    WITH media_por_clase AS (
        SELECT
            gene_id_base,
            gene_name,
            cancer_type,
            AVG(log2_tpm) AS media_clase
        FROM {TABLA_LONG}
        GROUP BY gene_id_base, gene_name, cancer_type
    ),
    variabilidad AS (
        SELECT
            gene_id_base,
            gene_name,
            AVG(media_clase) AS media_global,
            STDDEV(media_clase) AS sd_entre_clases,
            MAX(media_clase) - MIN(media_clase) AS rango_entre_clases
        FROM media_por_clase
        GROUP BY gene_id_base, gene_name
    )
    SELECT
        gene_id_base,
        gene_name,
        ROUND(media_global, 4) AS media_global,
        ROUND(sd_entre_clases, 4) AS sd_entre_clases,
        ROUND(rango_entre_clases, 4) AS rango_entre_clases
    FROM variabilidad
    ORDER BY sd_entre_clases DESC
    LIMIT {TOP_GENES_VARIABLES}
""")

display(eda_genes_mas_variables)

guardar_refined(
    eda_genes_mas_variables,
    "refined_eda_genes_mas_variables",
    "genes_mas_variables"
)

# COMMAND ----------

# Gráfico de top 20 genes más variables entre clases

pdf_genes_var = eda_genes_mas_variables.limit(20).toPandas()

plt.figure(figsize=(14, 6))
plt.bar(pdf_genes_var["gene_name"], pdf_genes_var["sd_entre_clases"])
plt.title("Top 20 genes con mayor variabilidad promedio entre tipos de cáncer")
plt.xlabel("Gen")
plt.ylabel("Desviación estándar entre clases")
plt.xticks(rotation=60, ha="right")
plt.tight_layout()

ruta_grafico_genes = f"{REFINED_VISUALIZATIONS_PATH}/top20_genes_variables.png"
plt.savefig(ruta_grafico_genes, dpi=300, bbox_inches="tight")
plt.show()

print("Gráfico guardado en:", ruta_grafico_genes)

# COMMAND ----------

# MAGIC %md
# MAGIC 9. Revisión de pacientes con más de una muestra

# COMMAND ----------

# Pacientes con más de una muestra

eda_muestras_por_paciente = spark.sql(f"""
    WITH muestras_paciente AS (
        SELECT
            patient_id,
            cancer_type,
            COUNT(DISTINCT sample_id) AS n_muestras
        FROM {TABLA_LONG}
        GROUP BY patient_id, cancer_type
    )
    SELECT
        cancer_type,
        COUNT(*) AS n_pacientes,
        SUM(CASE WHEN n_muestras > 1 THEN 1 ELSE 0 END) AS pacientes_con_mas_de_una_muestra,
        MAX(n_muestras) AS max_muestras_por_paciente
    FROM muestras_paciente
    GROUP BY cancer_type
    ORDER BY pacientes_con_mas_de_una_muestra DESC
""")

display(eda_muestras_por_paciente)

guardar_refined(
    eda_muestras_por_paciente,
    "refined_eda_muestras_por_paciente",
    "muestras_por_paciente"
)

# COMMAND ----------

# Verificación final de tablas y archivos refined

print("Tablas refined_eda creadas:")
display(spark.sql("SHOW TABLES IN workspace.default LIKE 'refined_eda*'"))

print("Contenido físico en refined/eda_outputs:")
display(dbutils.fs.ls(REFINED_EDA_PATH))

print("Contenido físico en refined/visualizations:")
display(dbutils.fs.ls(REFINED_VISUALIZATIONS_PATH))