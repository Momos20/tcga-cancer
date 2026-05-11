# Databricks notebook source
# MAGIC %run ./00_configuracion

# COMMAND ----------

#Importar librerías
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

print("Rutas del proyecto:")
print("RAW_RNASEQ_PATH:", RAW_RNASEQ_PATH)
print("RAW_METADATA_PATH:", RAW_METADATA_PATH)
print("TRUSTED_LONG_PATH:", TRUSTED_LONG_PATH)

# COMMAND ----------

# DBTITLE 1,Cell 3
# Leer metadatos oficiales de las 18 clases
df_meta = spark.read.csv(
    f"{RAW_METADATA_PATH}/metadatos_tcga_oficial_18_clases.csv",
    header=True,
    inferSchema=True
)

print("Columnas de metadatos:")
print(df_meta.columns)

print("Total de registros en metadatos oficiales:")
print(df_meta.count())

display(df_meta.limit(5))

print("Distribución por tipo de cáncer:")
display(
    df_meta
    .groupBy("cancer_type")
    .agg(F.count("*").alias("n_archivos"))
    .orderBy(F.desc("n_archivos"))
)

# COMMAND ----------

# Esto es importante para que Spark no infiera mal los tipos
# Esquema esperado de los archivos RNA-Seq STAR Counts
schema_rnaseq = StructType([
    StructField("gene_id", StringType(), True),
    StructField("gene_name", StringType(), True),
    StructField("gene_type", StringType(), True),
    StructField("unstranded", DoubleType(), True),
    StructField("stranded_first", DoubleType(), True),
    StructField("stranded_second", DoubleType(), True),
    StructField("tpm_unstranded", DoubleType(), True),
    StructField("fpkm_unstranded", DoubleType(), True),
    StructField("fpkm_uq_unstranded", DoubleType(), True)
])

# COMMAND ----------

# DBTITLE 1,Cell 5
# Leer archivos RNA-Seq completos desde raw/rnaseq
df_rnaseq_raw = (
    spark.read
    .option("header", True)
    .option("sep", "\t")
    .option("recursiveFileLookup", True)
    .schema(schema_rnaseq)
    .csv(RAW_RNASEQ_PATH)
    .withColumn("source_file", F.col("_metadata.file_path"))
)

print("Lectura inicial creada.")

display(df_rnaseq_raw.limit(10))

# COMMAND ----------

# Como cada archivo quedó dentro de una carpeta con su file_id, extraemos ese ID desde la ruta.
# Extraer file_id desde source_file

patron_uuid = r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"

df_rnaseq_raw = df_rnaseq_raw.withColumn(
    "file_id",
    F.regexp_extract(F.col("source_file"), patron_uuid, 1)
)

print("Validación de file_id extraído:")
display(
    df_rnaseq_raw
    .select("source_file", "file_id")
    .distinct()
    .limit(10)
)

# COMMAND ----------

# Limpieza de RNA-Seq

df_rnaseq_limpio = (
    df_rnaseq_raw
    # Eliminar filas técnicas N_unmapped, N_multimapping, N_noFeature, etc.
    .filter(~F.col("gene_id").startswith("N_"))

    # Conservar solo genes protein_coding
    .filter(F.col("gene_type") == "protein_coding")

    # Normalizar Ensembl ID quitando versión
    .withColumn(
        "gene_id_base",
        F.split(F.col("gene_id"), "\\.").getItem(0)
    )

    # Reemplazar TPM nulo por 0
    .withColumn(
        "tpm_unstranded",
        F.coalesce(F.col("tpm_unstranded"), F.lit(0.0))
    )

    # Transformación log2(TPM + 1)
    .withColumn(
        "log2_tpm",
        F.log2(F.col("tpm_unstranded") + F.lit(1.0))
    )
)

print("Datos RNA-Seq limpiados.")

display(df_rnaseq_limpio.limit(10))

# COMMAND ----------

# Unir RNA-Seq limpio con metadatos
columnas_meta = [
    "file_id",
    "file_name",
    "file_size",
    "case_id",
    "case_submitter_id",
    "sample_id",
    "sample_submitter_id",
    "sample_type",
    "project_id",
    "cancer_type",
    "cancer_name"
]

df_meta_sel = df_meta.select(*[c for c in columnas_meta if c in df_meta.columns])

df_trusted_long = (
    df_rnaseq_limpio
    .join(df_meta_sel, on="file_id", how="inner")
)

print("Datos unidos con metadatos.")

display(df_trusted_long.limit(10))

# COMMAND ----------

# Crear patient_id y filtrar dataset oficial
df_trusted_long = (
    df_trusted_long
    .withColumn(
        "patient_id",
        F.coalesce(
            F.col("case_submitter_id"),
            F.substring(F.col("sample_submitter_id"), 1, 12),
            F.col("sample_id")
        )
    )
    .filter(F.col("cancer_type").isin(CLASES_PRINCIPALES))
    .filter(F.lower(F.col("sample_type")).contains("primary tumor"))
)

columnas_finales = [
    "file_id",
    "file_name",
    "file_size",
    "case_id",
    "case_submitter_id",
    "sample_id",
    "sample_submitter_id",
    "patient_id",
    "sample_type",
    "project_id",
    "cancer_type",
    "cancer_name",
    "gene_id_base",
    "gene_name",
    "gene_type",
    "tpm_unstranded",
    "log2_tpm",
    "source_file"
]

df_trusted_long = df_trusted_long.select(*columnas_finales)

print("Dataset trusted filtrado.")

display(df_trusted_long.limit(10))

# COMMAND ----------

# Validaciones de calidad del dataset trusted

print("Número de muestras únicas:")
display(
    df_trusted_long
    .select("sample_id")
    .distinct()
    .agg(F.count("*").alias("n_muestras_unicas"))
)

print("Número de pacientes únicos:")
display(
    df_trusted_long
    .select("patient_id")
    .distinct()
    .agg(F.count("*").alias("n_pacientes_unicos"))
)

print("Número de genes únicos:")
display(
    df_trusted_long
    .select("gene_id_base")
    .distinct()
    .agg(F.count("*").alias("n_genes_unicos"))
)

print("Distribución por tipo de cáncer:")
display(
    df_trusted_long
    .select("sample_id", "cancer_type")
    .distinct()
    .groupBy("cancer_type")
    .agg(F.count("*").alias("n_muestras"))
    .orderBy(F.desc("n_muestras"))
)

print("Validación de tipos de muestra:")
display(
    df_trusted_long
    .select("sample_id", "sample_type")
    .distinct()
    .groupBy("sample_type")
    .agg(F.count("*").alias("n_muestras"))
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F
# Crear tabla candidata de muestras a partir de df_trusted_long


df_samples_candidata = (
    df_trusted_long
    .select(
        "file_id",
        "file_name",
        "file_size",
        "case_id",
        "case_submitter_id",
        "sample_id",
        "sample_submitter_id",
        "patient_id",
        "sample_type",
        "project_id",
        "cancer_type",
        "cancer_name"
    )
    .distinct()
)

print("Filas candidatas en tabla de muestras:")
print(df_samples_candidata.count())

print("Muestras únicas candidatas:")
display(
    df_samples_candidata
    .agg(
        F.count("*").alias("filas_samples_candidata"),
        F.countDistinct("sample_id").alias("muestras_unicas"),
        F.countDistinct("patient_id").alias("pacientes_unicos")
    )
)


# COMMAND ----------

# Identificar duplicados por sample_id
duplicados_sample = (
    df_samples_candidata
    .groupBy("sample_id")
    .agg(
        F.count("*").alias("n_filas"),
        F.countDistinct("file_id").alias("n_file_ids"),
        F.countDistinct("patient_id").alias("n_patient_ids"),
        F.first("cancer_type", ignorenulls=True).alias("cancer_type")
    )
    .filter(F.col("n_filas") > 1)
    .orderBy(F.desc("n_filas"))
)

print("Número de sample_id duplicados:")
print(duplicados_sample.count())

display(duplicados_sample.limit(20))

# COMMAND ----------


# Seleccionar un único archivo por sample_id

ventana_sample = (
    Window
    .partitionBy("sample_id")
    .orderBy(
        F.col("file_size").desc_nulls_last(),
        F.col("file_id").asc()
    )
)

df_samples_final = (
    df_samples_candidata
    .withColumn("rn", F.row_number().over(ventana_sample))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print("Tabla de muestras después de deduplicar:")
display(
    df_samples_final
    .agg(
        F.count("*").alias("filas_samples_final"),
        F.countDistinct("sample_id").alias("muestras_unicas_final"),
        F.countDistinct("patient_id").alias("pacientes_unicos_final")
    )
)

print("Distribución final por tipo de cáncer:")
display(
    df_samples_final
    .groupBy("cancer_type")
    .agg(F.count("*").alias("n_muestras"))
    .orderBy(F.desc("n_muestras"))
)

# COMMAND ----------

#Filtrar tabla long usando solo el file_id seleccionado

df_ids_finales = (
    df_samples_final
    .select("sample_id", "file_id")
    .dropDuplicates()
)

df_trusted_long = (
    df_trusted_long
    .join(df_ids_finales, on=["sample_id", "file_id"], how="inner")
)

print("Registros long después de filtrar muestras deduplicadas:")
print(df_trusted_long.count())

# COMMAND ----------


# Garantizar unicidad sample_id + gene_id_base

duplicados_sample_gene = (
    df_trusted_long
    .groupBy("sample_id", "gene_id_base")
    .agg(F.count("*").alias("n"))
    .filter(F.col("n") > 1)
)

print("Duplicados sample_id + gene_id_base antes de agregación:")
print(duplicados_sample_gene.count())

# Agregación defensiva:
# si por alguna razón queda más de una fila por muestra-gen,
# se promedia la expresión.
columnas_muestra_gen = [
    "file_id",
    "file_name",
    "file_size",
    "case_id",
    "case_submitter_id",
    "sample_id",
    "sample_submitter_id",
    "patient_id",
    "sample_type",
    "project_id",
    "cancer_type",
    "cancer_name",
    "gene_id_base"
]

df_trusted_long = (
    df_trusted_long
    .groupBy(*columnas_muestra_gen)
    .agg(
        F.first("gene_name", ignorenulls=True).alias("gene_name"),
        F.first("gene_type", ignorenulls=True).alias("gene_type"),
        F.avg("tpm_unstranded").alias("tpm_unstranded"),
        F.avg("log2_tpm").alias("log2_tpm"),
        F.first("source_file", ignorenulls=True).alias("source_file")
    )
)

print("Registros long después de agregación defensiva:")
print(df_trusted_long.count())

# COMMAND ----------

# Validación final de consistencia


print("Validación final después de deduplicación:")

display(
    df_trusted_long
    .agg(
        F.countDistinct("sample_id").alias("muestras_unicas_long"),
        F.countDistinct("patient_id").alias("pacientes_unicos_long"),
        F.countDistinct("gene_id_base").alias("genes_unicos_long")
    )
)

display(
    df_samples_final
    .agg(
        F.count("*").alias("filas_samples_final"),
        F.countDistinct("sample_id").alias("muestras_unicas_samples"),
        F.countDistinct("patient_id").alias("pacientes_unicos_samples")
    )
)

# COMMAND ----------



# COMMAND ----------

# Guardar dataset limpio en zona trusted como Delta
(
    df_trusted_long
    .repartition("cancer_type")
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("cancer_type")
    .save(TRUSTED_LONG_PATH)
)

print("Dataset trusted guardado en:")
print(TRUSTED_LONG_PATH)

# COMMAND ----------

# DBTITLE 1,Cell 12
# Catalogar tabla trusted para SparkSQL
# Leer desde el Volume y crear tabla managed en Unity Catalog
spark.sql("DROP TABLE IF EXISTS workspace.default.trusted_tcga_rnaseq_long_18_clases")

df_from_volume = spark.read.format("delta").load(TRUSTED_LONG_PATH)

(
    df_from_volume
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("cancer_type")
    .saveAsTable("workspace.default.trusted_tcga_rnaseq_long_18_clases")
)

print("Tabla catalogada:")
print("workspace.default.trusted_tcga_rnaseq_long_18_clases")

print("\nVerificación de registros en tabla catalogada:")
display(
    spark.sql(
        "SELECT cancer_type, COUNT(DISTINCT sample_id) as n_muestras "
        "FROM workspace.default.trusted_tcga_rnaseq_long_18_clases "
        "GROUP BY cancer_type ORDER BY n_muestras DESC"
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 13
# Crear tabla trusted de muestras únicas
df_samples = df_samples_final

# Validación rápida antes de guardar
print("Validación de df_samples antes de guardar:")

display(
    df_samples
    .agg(
        F.count("*").alias("filas_samples"),
        F.countDistinct("sample_id").alias("muestras_unicas"),
        F.countDistinct("patient_id").alias("pacientes_unicos")
    )
)

# Eliminar tabla anterior si existe
spark.sql("DROP TABLE IF EXISTS workspace.default.trusted_tcga_samples_18_clases")

# Guardar tabla trusted de muestras únicas
(
    df_samples
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.trusted_tcga_samples_18_clases")
)

print("Tabla de muestras creada correctamente:")
print("workspace.default.trusted_tcga_samples_18_clases")

# Distribución final por clase
display(
    df_samples
    .groupBy("cancer_type")
    .agg(F.count("*").alias("n_muestras"))
    .orderBy(F.desc("n_muestras"))
)

# COMMAND ----------

# DBTITLE 1,Cell 14
# Crear diccionario de genes

df_genes = (
    df_trusted_long
    .select(
        "gene_id_base",
        "gene_name",
        "gene_type"
    )
    .distinct()
)

spark.sql("DROP TABLE IF EXISTS workspace.default.trusted_tcga_gene_dictionary")

(
    df_genes
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.trusted_tcga_gene_dictionary")
)

print("Tabla de diccionario de genes creada:")
print("workspace.default.trusted_tcga_gene_dictionary")
print(f"Total de genes únicos: {df_genes.count()}")

display(df_genes.limit(10))

# COMMAND ----------

# Validación final usando SparkSQL

display(
    spark.sql("""
        SELECT cancer_type, COUNT(DISTINCT sample_id) AS n_muestras
        FROM workspace.default.trusted_tcga_rnaseq_long_18_clases
        GROUP BY cancer_type
        ORDER BY n_muestras DESC
    """)
)

display(
    spark.sql("""
        SELECT COUNT(DISTINCT sample_id) AS muestras,
               COUNT(DISTINCT patient_id) AS pacientes,
               COUNT(DISTINCT gene_id_base) AS genes
        FROM workspace.default.trusted_tcga_rnaseq_long_18_clases
    """)
)