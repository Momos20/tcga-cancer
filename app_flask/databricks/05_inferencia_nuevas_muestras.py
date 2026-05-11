# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Inferencia de nuevas muestras RNA-Seq
# MAGIC
# MAGIC Este notebook es una plantilla para ejecutar inferencia real en Databricks.
# MAGIC La app Flask valida y guarda el archivo; la predicción real debe mantener el mismo preprocesamiento del entrenamiento.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

# COMMAND ----------

dbutils.widgets.text("input_path", "")
dbutils.widgets.text("output_table", "workspace.default.refined_predicciones_nuevas_muestras")
dbutils.widgets.text("model_path", "")
dbutils.widgets.text("matrix_table", "workspace.default.refined_matriz_1000_genes")

input_path = dbutils.widgets.get("input_path")
output_table = dbutils.widgets.get("output_table")
model_path = dbutils.widgets.get("model_path")
matrix_table = dbutils.widgets.get("matrix_table")

if not input_path:
    raise ValueError("Debe indicar input_path con la ruta del CSV.")
if not model_path:
    raise ValueError("Debe indicar model_path con la ruta del modelo SparkML guardado.")

# COMMAND ----------

df_input = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

required = {"sample_id", "patient_id", "gene_id_base", "log2_tpm"}
missing = required - set(df_input.columns)
if missing:
    raise ValueError(f"Faltan columnas requeridas: {missing}")

df_input = (
    df_input
    .select(
        F.col("sample_id").cast("string"),
        F.col("patient_id").cast("string"),
        F.col("gene_id_base").cast("string"),
        F.col("log2_tpm").cast("double"),
    )
    .filter(F.col("sample_id").isNotNull())
    .filter(F.col("patient_id").isNotNull())
    .filter(F.col("gene_id_base").isNotNull())
    .filter(F.col("log2_tpm").isNotNull())
)

# COMMAND ----------

df_ref = spark.table(matrix_table)
id_cols = {"sample_id", "patient_id", "cancer_type"}
genes_modelo = [c for c in df_ref.columns if c not in id_cols]

df_long_modelo = df_input.filter(F.col("gene_id_base").isin(genes_modelo))

if df_long_modelo.select("gene_id_base").distinct().count() == 0:
    raise ValueError("Ningún gen del archivo coincide con los genes esperados por el modelo.")

df_matriz = (
    df_long_modelo
    .groupBy("sample_id", "patient_id")
    .pivot("gene_id_base", genes_modelo)
    .agg(F.first("log2_tpm"))
    .fillna(0.0)
    .withColumn("cancer_type", F.lit("DESCONOCIDO"))
    .select("sample_id", "patient_id", "cancer_type", *genes_modelo)
)

# COMMAND ----------

modelo = PipelineModel.load(model_path)
labels = list(modelo.stages[0].labels)

pred = (
    modelo.transform(df_matriz)
    .withColumn("probability_array", vector_to_array(F.col("probability")))
)

labels_expr = F.array(*[F.lit(label) for label in labels])

resultado = (
    pred
    .withColumn("prediction_int", F.col("prediction").cast("int"))
    .withColumn("cancer_predicho", labels_expr[F.col("prediction_int")])
    .withColumn("probabilidad_predicha", F.col("probability_array")[F.col("prediction_int")])
    .select(
        "sample_id",
        "patient_id",
        "cancer_predicho",
        "probabilidad_predicha",
        "prediction",
        "probability_array",
    )
)

display(resultado)

# COMMAND ----------

(
    resultado
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(output_table)
)

print("Predicciones guardadas en:", output_table)
