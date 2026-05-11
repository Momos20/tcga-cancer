# Databricks notebook source
# MAGIC %run ./00_configuracion

# COMMAND ----------


# 1. Configuración general e imports

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.functions import vector_to_array

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    precision_recall_fscore_support,
    classification_report,
    confusion_matrix,
    roc_auc_score,
    average_precision_score,
    roc_curve,
    precision_recall_curve,
    auc
)

from sklearn.preprocessing import label_binarize

spark.conf.set("spark.sql.shuffle.partitions", "200")

print("Configuración cargada correctamente.")
print("Ruta trusted:", TRUSTED_PATH)
print("Ruta refined:", REFINED_PATH)
print("Ruta modelos:", MODELS_PATH)

# COMMAND ----------

 # 2. Lectura de tablas trusted

df_long = spark.table("workspace.default.trusted_tcga_rnaseq_long_18_clases")
df_samples = spark.table("workspace.default.trusted_tcga_samples_18_clases")

print("Muestras únicas:", df_samples.count())
print("Pacientes únicos:", df_samples.select("patient_id").distinct().count())
print("Genes únicos:", df_long.select("gene_id_base").distinct().count())

print("Distribución por tipo de cáncer:")
display(
    df_samples
    .groupBy("cancer_type")
    .agg(
        F.count("*").alias("n_muestras"),
        F.countDistinct("patient_id").alias("n_pacientes")
    )
    .orderBy(F.desc("n_muestras"))
)

# COMMAND ----------

# 2.1 Validación rápida de consistencia
validacion_consistencia = spark.sql("""
    SELECT
        (SELECT COUNT(DISTINCT sample_id)
         FROM workspace.default.trusted_tcga_rnaseq_long_18_clases) AS muestras_unicas_long,

        (SELECT COUNT(*)
         FROM workspace.default.trusted_tcga_samples_18_clases) AS filas_samples,

        (SELECT COUNT(DISTINCT sample_id)
         FROM workspace.default.trusted_tcga_samples_18_clases) AS muestras_unicas_samples,

        (SELECT COUNT(DISTINCT patient_id)
         FROM workspace.default.trusted_tcga_rnaseq_long_18_clases) AS pacientes_unicos_long,

        (SELECT COUNT(DISTINCT gene_id_base)
         FROM workspace.default.trusted_tcga_rnaseq_long_18_clases) AS genes_unicos_long
""")

display(validacion_consistencia)

# Verificar que no haya duplicados por muestra en la tabla de muestras
duplicados_sample = (
    df_samples
    .groupBy("sample_id")
    .agg(F.count("*").alias("n_filas"))
    .filter(F.col("n_filas") > 1)
)

print("Duplicados por sample_id:", duplicados_sample.count())

# COMMAND ----------

# DBTITLE 1,Selección de genes más variables
# 3. Selección de genes más variables entre clases
N_GENES = 100  # Reducido a 100 para evitar MODEL_SIZE_OVERFLOW_EXCEPTION en serverless (límite 268 MB)

try:
    df_genes_variables = spark.table("workspace.default.refined_eda_genes_mas_variables")
    print("Usando tabla refined_eda_genes_mas_variables del EDA.")
except Exception as e:
    print("No se encontró refined_eda_genes_mas_variables. Se calculará desde la tabla trusted.")
    print("Detalle:", e)

    df_genes_variables = spark.sql("""
        WITH media_por_clase AS (
            SELECT
                gene_id_base,
                gene_name,
                cancer_type,
                AVG(log2_tpm) AS media_clase
            FROM workspace.default.trusted_tcga_rnaseq_long_18_clases
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
        SELECT *
        FROM variabilidad
        ORDER BY sd_entre_clases DESC
    """)

genes_modelo = [
    row["gene_id_base"]
    for row in (
        df_genes_variables
        .orderBy(F.desc("sd_entre_clases"))
        .limit(N_GENES)
        .select("gene_id_base")
        .collect()
    )
]

print("Número de genes seleccionados:", len(genes_modelo))
print("Primeros genes seleccionados:", genes_modelo[:10])

# COMMAND ----------

# 4. Construcción de matriz muestra × genes
df_long_modelo = (
    df_long
    .filter(F.col("gene_id_base").isin(genes_modelo))
    .select(
        "sample_id",
        "patient_id",
        "cancer_type",
        "gene_id_base",
        "log2_tpm"
    )
)

print("Registros usados para matriz ML:", df_long_modelo.count())

df_matriz = (
    df_long_modelo
    .groupBy("sample_id", "patient_id", "cancer_type")
    .pivot("gene_id_base", genes_modelo)
    .agg(F.first("log2_tpm"))
    .fillna(0.0)
)

n_filas = df_matriz.count()
n_columnas = len(df_matriz.columns)

print("Filas matriz:", n_filas)
print("Columnas matriz:", n_columnas)

display(df_matriz.limit(5))

# COMMAND ----------

# DBTITLE 1,Guardar matriz de modelado en Unity Catalog
# 4.1 Guardar matriz de modelado en Unity Catalog

# Eliminar tabla si existe
spark.sql("DROP TABLE IF EXISTS workspace.default.refined_ml_matriz_100_genes")

# Guardar directamente como tabla Unity Catalog
(
    df_matriz
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.refined_ml_matriz_100_genes")
)

print("Tabla creada: workspace.default.refined_ml_matriz_100_genes")
print("Dimensiones:", df_matriz.count(), "filas x", len(df_matriz.columns), "columnas")

# COMMAND ----------

# MAGIC %md
# MAGIC Proporciones usadas:
# MAGIC
# MAGIC - 70% entrenamiento.
# MAGIC - 15% validación.
# MAGIC - 15% prueba.

# COMMAND ----------

# DBTITLE 1,Partición train / validation / test
# 5. Partición train / validation / test por paciente y clase

df_pacientes = (
    df_matriz
    .select("patient_id", "cancer_type")
    .dropDuplicates()
)

window_clase = Window.partitionBy("cancer_type").orderBy(F.rand(seed=42))

df_pacientes_split = (
    df_pacientes
    .withColumn("rn", F.row_number().over(window_clase))
    .withColumn("n_clase", F.count("*").over(Window.partitionBy("cancer_type")))
    .withColumn("proporcion", F.col("rn") / F.col("n_clase"))
    .withColumn(
        "split",
        F.when(F.col("proporcion") <= 0.70, F.lit("train"))
         .when(F.col("proporcion") <= 0.85, F.lit("validation"))
         .otherwise(F.lit("test"))
    )
    .select("patient_id", "split")
)

df_matriz_split = (
    df_matriz
    .join(df_pacientes_split, on="patient_id", how="inner")
)

df_train = df_matriz_split.filter(F.col("split") == "train").drop("split")
df_val = df_matriz_split.filter(F.col("split") == "validation").drop("split")
df_test = df_matriz_split.filter(F.col("split") == "test").drop("split")

print("Train:", df_train.count())
print("Validation:", df_val.count())
print("Test:", df_test.count())

print("Distribución train:")
display(df_train.groupBy("cancer_type").agg(F.count("*").alias("n")).orderBy(F.desc("n")))

print("Distribución validation:")
display(df_val.groupBy("cancer_type").agg(F.count("*").alias("n")).orderBy(F.desc("n")))

print("Distribución test:")
display(df_test.groupBy("cancer_type").agg(F.count("*").alias("n")).orderBy(F.desc("n")))

# COMMAND ----------

# 5.1 Validación de fuga por paciente

train_patients = set([r["patient_id"] for r in df_train.select("patient_id").distinct().collect()])
val_patients = set([r["patient_id"] for r in df_val.select("patient_id").distinct().collect()])
test_patients = set([r["patient_id"] for r in df_test.select("patient_id").distinct().collect()])

print("Cruce train-validation:", len(train_patients.intersection(val_patients)))
print("Cruce train-test:", len(train_patients.intersection(test_patients)))
print("Cruce validation-test:", len(val_patients.intersection(test_patients)))

# COMMAND ----------

# 6. Columnas y transformadores SparkML

columnas_id = ["sample_id", "patient_id", "cancer_type"]
columnas_genes = [col for col in df_matriz.columns if col not in columnas_id]

print("Número de variables predictoras:", len(columnas_genes))

label_indexer = StringIndexer(
    inputCol="cancer_type",
    outputCol="label",
    handleInvalid="keep"
)

assembler = VectorAssembler(
    inputCols=columnas_genes,
    outputCol="features_raw",
    handleInvalid="keep"
)

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features_scaled",
    withMean=False,
    withStd=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC Se reportan métricas globales y por clase:
# MAGIC
# MAGIC - Accuracy.
# MAGIC - Balanced Accuracy.
# MAGIC - Precision macro y weighted.
# MAGIC - Recall macro y weighted.
# MAGIC - F1 macro y weighted.
# MAGIC - ROC-AUC macro One-vs-Rest.
# MAGIC - PR-AUC macro One-vs-Rest.
# MAGIC - Matriz de confusión.
# MAGIC
# MAGIC El criterio principal de selección será **F1 macro en validación**, porque el dataset presenta desbalance entre clases.

# COMMAND ----------

# DBTITLE 1,Funciones auxiliares para evaluación
# 7. Funciones auxiliares para evaluación

def obtener_labels_pipeline(modelo_pipeline):
    """
    Obtiene las etiquetas originales aprendidas por StringIndexer.
    Se asume que StringIndexer es la primera etapa del Pipeline.
    """
    return list(modelo_pipeline.stages[0].labels)


def predicciones_a_pandas(predicciones, label_names):
    """
    Convierte predicciones Spark a pandas, incluyendo probabilidades como matriz numpy.
    """
    pred_pd = (
        predicciones
        .select(
            "sample_id",
            "patient_id",
            "cancer_type",
            "label",
            "prediction",
            vector_to_array("probability").alias("probability_array")
        )
        .toPandas()
    )

    pred_pd["label"] = pred_pd["label"].astype(int)
    pred_pd["prediction"] = pred_pd["prediction"].astype(int)
    pred_pd["cancer_predicho"] = pred_pd["prediction"].apply(lambda i: label_names[int(i)] if int(i) < len(label_names) else "DESCONOCIDO")

    probas = np.vstack(pred_pd["probability_array"].apply(lambda x: np.array(x, dtype=float)).values)

    return pred_pd, probas


def calcular_metricas_sklearn(nombre_modelo, nombre_split, predicciones, label_names):
    """
    Calcula métricas globales para clasificación multiclase.
    """
    pred_pd, probas = predicciones_a_pandas(predicciones, label_names)

    y_true = pred_pd["label"].values
    y_pred = pred_pd["prediction"].values
    clases = list(range(len(label_names)))

    acc = accuracy_score(y_true, y_pred)
    bal_acc = balanced_accuracy_score(y_true, y_pred)

    precision_macro, recall_macro, f1_macro, _ = precision_recall_fscore_support(
        y_true, y_pred, labels=clases, average="macro", zero_division=0
    )

    precision_weighted, recall_weighted, f1_weighted, _ = precision_recall_fscore_support(
        y_true, y_pred, labels=clases, average="weighted", zero_division=0
    )

    # AUC multiclase One-vs-Rest. Si alguna clase no aparece en un split, se controla el error.
    try:
        roc_auc_macro = roc_auc_score(
            y_true,
            probas,
            labels=clases,
            multi_class="ovr",
            average="macro"
        )
    except Exception:
        roc_auc_macro = np.nan

    try:
        y_bin = label_binarize(y_true, classes=clases)
        pr_auc_macro = average_precision_score(y_bin, probas, average="macro")
    except Exception:
        pr_auc_macro = np.nan

    return {
        "modelo": nombre_modelo,
        "split": nombre_split,
        "accuracy": float(acc),
        "balanced_accuracy": float(bal_acc),
        "precision_macro": float(precision_macro),
        "recall_macro": float(recall_macro),
        "f1_macro": float(f1_macro),
        "precision_weighted": float(precision_weighted),
        "recall_weighted": float(recall_weighted),
        "f1_weighted": float(f1_weighted),
        "roc_auc_macro_ovr": float(roc_auc_macro) if not np.isnan(roc_auc_macro) else None,
        "pr_auc_macro_ovr": float(pr_auc_macro) if not np.isnan(pr_auc_macro) else None
    }


def evaluar_modelo(nombre_modelo, modelo_entrenado, df_train, df_val, df_test):
    """
    Evalúa un modelo entrenado en train, validation y test.
    Retorna métricas en Spark DataFrame y diccionario de predicciones.
    """
    label_names = obtener_labels_pipeline(modelo_entrenado)
    resultados = []
    predicciones = {}

    for nombre_split, df_split in {
        "train": df_train,
        "validation": df_val,
        "test": df_test
    }.items():

        pred = modelo_entrenado.transform(df_split)
        predicciones[nombre_split] = pred

        metricas = calcular_metricas_sklearn(
            nombre_modelo=nombre_modelo,
            nombre_split=nombre_split,
            predicciones=pred,
            label_names=label_names
        )

        resultados.append(metricas)

    df_resultados = spark.createDataFrame(pd.DataFrame(resultados))

    return df_resultados, predicciones, label_names

# COMMAND ----------

# MAGIC %md
# MAGIC Se entrenan tres modelos de SparkML:
# MAGIC
# MAGIC 1. **LogisticRegression multinomial**: modelo lineal regularizado, adecuado para datos de alta dimensionalidad.
# MAGIC 2. **RandomForestClassifier**: ensamble no lineal que permite comparar frente a modelos lineales y explorar importancia de variables.
# MAGIC 3. **NaiveBayes**: modelo probabilístico rápido, multiclase y compatible con variables no negativas.

# COMMAND ----------

# 8.1 Modelo 1: LogisticRegression multinomial

lr = LogisticRegression(
    featuresCol="features_scaled",
    labelCol="label",
    predictionCol="prediction",
    probabilityCol="probability",
    family="multinomial",
    maxIter=100,
    regParam=0.1,
    elasticNetParam=0.0
)

pipeline_lr = Pipeline(stages=[
    label_indexer,
    assembler,
    scaler,
    lr
])

modelo_lr = pipeline_lr.fit(df_train)

metricas_lr, predicciones_lr, labels_lr = evaluar_modelo(
    "LogisticRegression_multinomial",
    modelo_lr,
    df_train,
    df_val,
    df_test
)

display(metricas_lr)

# COMMAND ----------

# 8.2 Modelo 2: RandomForestClassifier

rf = RandomForestClassifier(
    featuresCol="features_raw",
    labelCol="label",
    predictionCol="prediction",
    probabilityCol="probability",
    numTrees=100,
    maxDepth=8,
    maxBins=32,
    seed=42
)

pipeline_rf = Pipeline(stages=[
    label_indexer,
    assembler,
    rf
])

modelo_rf = pipeline_rf.fit(df_train)

metricas_rf, predicciones_rf, labels_rf = evaluar_modelo(
    "RandomForestClassifier",
    modelo_rf,
    df_train,
    df_val,
    df_test
)

display(metricas_rf)

# COMMAND ----------

# 8.3 Modelo 3: NaiveBayes

nb = NaiveBayes(
    featuresCol="features_raw",
    labelCol="label",
    predictionCol="prediction",
    probabilityCol="probability",
    modelType="multinomial",
    smoothing=1.0
)

pipeline_nb = Pipeline(stages=[
    label_indexer,
    assembler,
    nb
])

modelo_nb = pipeline_nb.fit(df_train)

metricas_nb, predicciones_nb, labels_nb = evaluar_modelo(
    "NaiveBayes",
    modelo_nb,
    df_train,
    df_val,
    df_test
)

display(metricas_nb)

# COMMAND ----------

# MAGIC %md
# MAGIC El mejor modelo se selecciona usando el conjunto de **validación**, no el conjunto de prueba. La métrica principal es **F1 macro**, porque el problema tiene clases desbalanceadas. Como criterio secundario se revisan **balanced accuracy**, **PR-AUC macro** y la brecha entre entrenamiento y validación.

# COMMAND ----------

# 9. Comparación global de modelos
metricas_modelos = (
    metricas_lr
    .unionByName(metricas_rf)
    .unionByName(metricas_nb)
)

# Agregar diagnóstico de brecha train-validation para detectar sobreajuste
metricas_pd = metricas_modelos.toPandas()

metricas_train = metricas_pd[metricas_pd["split"] == "train"][["modelo", "f1_macro", "accuracy"]].rename(
    columns={"f1_macro": "f1_macro_train", "accuracy": "accuracy_train"}
)

metricas_val = metricas_pd[metricas_pd["split"] == "validation"][["modelo", "f1_macro", "accuracy"]].rename(
    columns={"f1_macro": "f1_macro_validation", "accuracy": "accuracy_validation"}
)

brechas_pd = metricas_train.merge(metricas_val, on="modelo", how="inner")
brechas_pd["gap_f1_macro_train_validation"] = brechas_pd["f1_macro_train"] - brechas_pd["f1_macro_validation"]
brechas_pd["gap_accuracy_train_validation"] = brechas_pd["accuracy_train"] - brechas_pd["accuracy_validation"]

metricas_modelos = metricas_modelos.join(
    spark.createDataFrame(brechas_pd[["modelo", "gap_f1_macro_train_validation", "gap_accuracy_train_validation"]]),
    on="modelo",
    how="left"
)

display(
    metricas_modelos
    .orderBy("split", F.desc("f1_macro"))
)

# COMMAND ----------

# DBTITLE 1,Guardar métricas de modelos en refined
# 9.1 Guardar métricas de modelos en refined

# Eliminar tabla si existe
spark.sql("DROP TABLE IF EXISTS workspace.default.refined_metricas_modelos_sparkml")

# Guardar directamente como tabla Unity Catalog
(
    metricas_modelos
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.refined_metricas_modelos_sparkml")
)

print("Tabla creada: workspace.default.refined_metricas_modelos_sparkml")
print(f"Filas guardadas: {metricas_modelos.count()}")

# COMMAND ----------

# 9.2 Selección del mejor modelo según F1 macro en validación

mejor_fila = (
    metricas_modelos
    .filter(F.col("split") == "validation")
    .orderBy(
        F.desc("f1_macro"),
        F.desc("balanced_accuracy"),
        F.desc("pr_auc_macro_ovr")
    )
    .limit(1)
    .collect()[0]
)

mejor_modelo_nombre = mejor_fila["modelo"]

print("Mejor modelo según validación:")
print("Modelo:", mejor_modelo_nombre)
print("F1 macro:", mejor_fila["f1_macro"])
print("Balanced accuracy:", mejor_fila["balanced_accuracy"])
print("PR-AUC macro OvR:", mejor_fila["pr_auc_macro_ovr"])
print("ROC-AUC macro OvR:", mejor_fila["roc_auc_macro_ovr"])
print("Gap F1 macro train-validation:", mejor_fila["gap_f1_macro_train_validation"])

# COMMAND ----------

# 9.3 Seleccionar objeto y predicciones del mejor modelo

if mejor_modelo_nombre == "LogisticRegression_multinomial":
    modelo_mejor = modelo_lr
    predicciones_mejor = predicciones_lr
    labels_mejor = labels_lr
elif mejor_modelo_nombre == "RandomForestClassifier":
    modelo_mejor = modelo_rf
    predicciones_mejor = predicciones_rf
    labels_mejor = labels_rf
elif mejor_modelo_nombre == "NaiveBayes":
    modelo_mejor = modelo_nb
    predicciones_mejor = predicciones_nb
    labels_mejor = labels_nb
else:
    raise ValueError("Modelo no reconocido.")

pred_test_mejor = predicciones_mejor["test"]

print("Modelo final seleccionado:", mejor_modelo_nombre)
print("Etiquetas del modelo:", labels_mejor)

# COMMAND ----------

# MAGIC %md
# MAGIC Evaluación detallada del mejor modelo
# MAGIC
# MAGIC Se calcula:
# MAGIC
# MAGIC - Reporte por clase.
# MAGIC - Matriz de confusión.
# MAGIC - Errores por tipo de cáncer.
# MAGIC - Curvas ROC One-vs-Rest.
# MAGIC - Curvas Precision-Recall One-vs-Rest.

# COMMAND ----------

# 10. Reporte por clase del mejor modelo en test

pred_test_pd, probas_test = predicciones_a_pandas(pred_test_mejor, labels_mejor)

y_test = pred_test_pd["label"].values
y_pred_test = pred_test_pd["prediction"].values
clases = list(range(len(labels_mejor)))

reporte_dict = classification_report(
    y_test,
    y_pred_test,
    labels=clases,
    target_names=labels_mejor,
    output_dict=True,
    zero_division=0
)

reporte_clases_pd = pd.DataFrame(reporte_dict).T.reset_index().rename(columns={"index": "clase"})

display(spark.createDataFrame(reporte_clases_pd))

# COMMAND ----------

# DBTITLE 1,Guardar reporte por clase en refined
# 10.1 Guardar reporte por clase en refined

reporte_clases_spark = spark.createDataFrame(reporte_clases_pd)

# Eliminar tabla si existe
spark.sql("DROP TABLE IF EXISTS workspace.default.refined_reporte_clasificacion_por_clase")

# Guardar directamente como tabla Unity Catalog
(
    reporte_clases_spark
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.refined_reporte_clasificacion_por_clase")
)

print("Tabla creada: workspace.default.refined_reporte_clasificacion_por_clase")

# COMMAND ----------

# 10.2 Matriz de confusión del mejor modelo

cm = confusion_matrix(y_test, y_pred_test, labels=clases)

cm_pd = pd.DataFrame(cm, index=labels_mejor, columns=labels_mejor)
cm_long_pd = cm_pd.reset_index().melt(id_vars="index", var_name="predicted_class", value_name="n")
cm_long_pd = cm_long_pd.rename(columns={"index": "true_class"})

confusion_spark = spark.createDataFrame(cm_long_pd)

display(confusion_spark)

# COMMAND ----------

# DBTITLE 1,Guardar predicciones y matriz de confusión en refined
# 10.3 Guardar predicciones y matriz de confusión en refined

pred_test_save_pd = pred_test_pd.copy()
pred_test_save_pd["probability_array"] = pred_test_save_pd["probability_array"].apply(lambda x: [float(v) for v in x])

pred_test_save_spark = spark.createDataFrame(pred_test_save_pd[[
    "sample_id",
    "patient_id",
    "cancer_type",
    "cancer_predicho",
    "label",
    "prediction"
]])

# Eliminar tablas si existen
spark.sql("DROP TABLE IF EXISTS workspace.default.refined_predicciones_test_mejor_modelo")
spark.sql("DROP TABLE IF EXISTS workspace.default.refined_matriz_confusion_mejor_modelo")

# Guardar predicciones directamente como tabla Unity Catalog
(
    pred_test_save_spark
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.refined_predicciones_test_mejor_modelo")
)

# Guardar matriz de confusión directamente como tabla Unity Catalog
(
    confusion_spark
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.refined_matriz_confusion_mejor_modelo")
)

print("Tabla creada: workspace.default.refined_predicciones_test_mejor_modelo")
print("Tabla creada: workspace.default.refined_matriz_confusion_mejor_modelo")

# COMMAND ----------

# MAGIC %md
# MAGIC Visualizaciones finales

# COMMAND ----------

# 11.1 Comparación de modelos por F1 macro en validation y test

metricas_pd = metricas_modelos.toPandas()
metricas_plot = metricas_pd[metricas_pd["split"].isin(["validation", "test"])].copy()

# Tabla resumen para mostrar
metricas_resumen = metricas_plot[[
    "modelo", "split", "accuracy", "balanced_accuracy", "f1_macro", "f1_weighted", "roc_auc_macro_ovr", "pr_auc_macro_ovr"
]].sort_values(["split", "f1_macro"], ascending=[True, False])

display(spark.createDataFrame(metricas_resumen))

# Gráfico F1 macro
plt.figure(figsize=(10, 5))

for split in ["validation", "test"]:
    temp = metricas_plot[metricas_plot["split"] == split]
    plt.plot(temp["modelo"], temp["f1_macro"], marker="o", label=split)

plt.title("Comparación de modelos - F1 macro")
plt.xlabel("Modelo")
plt.ylabel("F1 macro")
plt.xticks(rotation=30, ha="right")
plt.legend()
plt.tight_layout()

ruta_fig_modelos = f"{REFINED_VISUALIZATIONS_PATH}/comparacion_modelos_f1_macro.png"
plt.savefig(ruta_fig_modelos, dpi=300, bbox_inches="tight")
plt.show()

print("Gráfico guardado en:", ruta_fig_modelos)

# COMMAND ----------

# 11.2 Matriz de confusión visual

plt.figure(figsize=(11, 9))
plt.imshow(cm, interpolation="nearest")
plt.title(f"Matriz de confusión - {mejor_modelo_nombre} - Test")
plt.colorbar()

plt.xticks(np.arange(len(labels_mejor)), labels_mejor, rotation=90)
plt.yticks(np.arange(len(labels_mejor)), labels_mejor)
plt.xlabel("Clase predicha")
plt.ylabel("Clase real")

# Escribir valores principales. Si son muchos, se mantiene legible por tamaño.
for i in range(cm.shape[0]):
    for j in range(cm.shape[1]):
        if cm[i, j] > 0:
            plt.text(j, i, str(cm[i, j]), ha="center", va="center", fontsize=7)

plt.tight_layout()

ruta_fig_cm = f"{REFINED_VISUALIZATIONS_PATH}/matriz_confusion_mejor_modelo.png"
plt.savefig(ruta_fig_cm, dpi=300, bbox_inches="tight")
plt.show()

print("Matriz de confusión guardada en:", ruta_fig_cm)

# COMMAND ----------

# 11.3 Curvas ROC y Precision-Recall One-vs-Rest

# Para no saturar la figura, se grafican las 6 clases con mayor soporte en test.
soporte_test = pred_test_pd["label"].value_counts().sort_values(ascending=False)
clases_a_graficar = soporte_test.head(6).index.tolist()

print("Clases graficadas:", [labels_mejor[i] for i in clases_a_graficar])

y_test_bin = label_binarize(y_test, classes=clases)

# Curva ROC
plt.figure(figsize=(8, 6))
for idx in clases_a_graficar:
    fpr, tpr, _ = roc_curve(y_test_bin[:, idx], probas_test[:, idx])
    roc_auc = auc(fpr, tpr)
    plt.plot(fpr, tpr, label=f"{labels_mejor[idx]} AUC={roc_auc:.3f}")

plt.plot([0, 1], [0, 1], linestyle="--", label="Clasificación aleatoria")
plt.title("Curvas ROC One-vs-Rest - Test")
plt.xlabel("FPR")
plt.ylabel("TPR / Recall")
plt.legend(loc="lower right", fontsize=8)
plt.tight_layout()

ruta_fig_roc = f"{REFINED_VISUALIZATIONS_PATH}/curvas_roc_ovr_mejor_modelo.png"
plt.savefig(ruta_fig_roc, dpi=300, bbox_inches="tight")
plt.show()

print("Curvas ROC guardadas en:", ruta_fig_roc)

# Curva Precision-Recall
plt.figure(figsize=(8, 6))
for idx in clases_a_graficar:
    precision, recall, _ = precision_recall_curve(y_test_bin[:, idx], probas_test[:, idx])
    pr_auc = auc(recall, precision)
    plt.plot(recall, precision, label=f"{labels_mejor[idx]} AUC={pr_auc:.3f}")

plt.title("Curvas Precision-Recall One-vs-Rest - Test")
plt.xlabel("Recall")
plt.ylabel("Precision")
plt.legend(loc="lower left", fontsize=8)
plt.tight_layout()

ruta_fig_pr = f"{REFINED_VISUALIZATIONS_PATH}/curvas_precision_recall_ovr_mejor_modelo.png"
plt.savefig(ruta_fig_pr, dpi=300, bbox_inches="tight")
plt.show()

print("Curvas Precision-Recall guardadas en:", ruta_fig_pr)

# COMMAND ----------

# MAGIC %md
# MAGIC El modelo seleccionado se guarda en la zona `models` para reproducibilidad.

# COMMAND ----------

# 12. Guardar mejor modelo SparkML

ruta_modelo_mejor = f"{MODELS_PATH}/{mejor_modelo_nombre}"

modelo_mejor.write().overwrite().save(ruta_modelo_mejor)

print("Mejor modelo guardado en:")
print(ruta_modelo_mejor)

# COMMAND ----------

# 13. Verificación final de salidas refined y models

print("Tablas refined generadas:")
display(spark.sql("SHOW TABLES IN workspace.default LIKE 'refined_*'"))

print("Contenido de refined/model_metrics:")
display(dbutils.fs.ls(REFINED_METRICS_PATH))

print("Contenido de refined/predictions:")
display(dbutils.fs.ls(REFINED_PREDICTIONS_PATH))

print("Contenido de refined/visualizations:")
display(dbutils.fs.ls(REFINED_VISUALIZATIONS_PATH))

print("Contenido de models:")
display(dbutils.fs.ls(MODELS_PATH))