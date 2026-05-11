# Databricks notebook source
# 00_configuracion
# Arquitectura batch en Databricks

BASE_PATH = "/Volumes/workspace/default/tcga_cancer_ml"

RAW_PATH = f"{BASE_PATH}/raw"
TRUSTED_PATH = f"{BASE_PATH}/trusted"
REFINED_PATH = f"{BASE_PATH}/refined"
MODELS_PATH = f"{BASE_PATH}/models"

RAW_RNASEQ_PATH = f"{RAW_PATH}/rnaseq"
RAW_METADATA_PATH = f"{RAW_PATH}/metadata"

TRUSTED_LONG_PATH = f"{TRUSTED_PATH}/rnaseq_long"
TRUSTED_MATRIX_PATH = f"{TRUSTED_PATH}/rnaseq_matrix"

REFINED_EDA_PATH = f"{REFINED_PATH}/eda_outputs"
REFINED_METRICS_PATH = f"{REFINED_PATH}/model_metrics"
REFINED_PREDICTIONS_PATH = f"{REFINED_PATH}/predictions"
REFINED_VISUALIZATIONS_PATH = f"{REFINED_PATH}/visualizations"

CLASES_PRINCIPALES = [
    "BRCA", "KIRC", "LUAD", "UCEC", "THCA", "HNSC",
    "LUSC", "PRAD", "LGG", "COAD", "SKCM", "STAD",
    "OV", "BLCA", "LIHC", "GBM", "KIRP", "CESC"
]

PROYECTOS_PRINCIPALES = [
    "TCGA-BRCA", "TCGA-KIRC", "TCGA-LUAD", "TCGA-UCEC",
    "TCGA-THCA", "TCGA-HNSC", "TCGA-LUSC", "TCGA-PRAD",
    "TCGA-LGG", "TCGA-COAD", "TCGA-SKCM", "TCGA-STAD",
    "TCGA-OV", "TCGA-BLCA", "TCGA-LIHC", "TCGA-GBM",
    "TCGA-KIRP", "TCGA-CESC"
]

mapa_cancer = {
    "TCGA-BRCA": "BRCA",
    "TCGA-KIRC": "KIRC",
    "TCGA-LUAD": "LUAD",
    "TCGA-UCEC": "UCEC",
    "TCGA-THCA": "THCA",
    "TCGA-HNSC": "HNSC",
    "TCGA-LUSC": "LUSC",
    "TCGA-PRAD": "PRAD",
    "TCGA-LGG": "LGG",
    "TCGA-COAD": "COAD",
    "TCGA-SKCM": "SKCM",
    "TCGA-STAD": "STAD",
    "TCGA-OV": "OV",
    "TCGA-BLCA": "BLCA",
    "TCGA-LIHC": "LIHC",
    "TCGA-GBM": "GBM",
    "TCGA-KIRP": "KIRP",
    "TCGA-CESC": "CESC"
}

mapa_nombre_cancer = {
    "TCGA-BRCA": "Breast invasive carcinoma",
    "TCGA-KIRC": "Kidney renal clear cell carcinoma",
    "TCGA-LUAD": "Lung adenocarcinoma",
    "TCGA-UCEC": "Uterine corpus endometrial carcinoma",
    "TCGA-THCA": "Thyroid carcinoma",
    "TCGA-HNSC": "Head and neck squamous cell carcinoma",
    "TCGA-LUSC": "Lung squamous cell carcinoma",
    "TCGA-PRAD": "Prostate adenocarcinoma",
    "TCGA-LGG": "Brain lower grade glioma",
    "TCGA-COAD": "Colon adenocarcinoma",
    "TCGA-SKCM": "Skin cutaneous melanoma",
    "TCGA-STAD": "Stomach adenocarcinoma",
    "TCGA-OV": "Ovarian serous cystadenocarcinoma",
    "TCGA-BLCA": "Bladder urothelial carcinoma",
    "TCGA-LIHC": "Liver hepatocellular carcinoma",
    "TCGA-GBM": "Glioblastoma multiforme",
    "TCGA-KIRP": "Kidney renal papillary cell carcinoma",
    "TCGA-CESC": "Cervical squamous cell carcinoma and endocervical adenocarcinoma"
}

print("Configuración cargada correctamente.")
print("Ruta base:", BASE_PATH)
print("Número de clases oficiales:", len(CLASES_PRINCIPALES))

# COMMAND ----------

# Crear estructura del datalake batch
# Crear el volumen si no existe
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.tcga_cancer_ml")

rutas = [
    BASE_PATH,
    RAW_PATH,
    TRUSTED_PATH,
    REFINED_PATH,
    MODELS_PATH,
    RAW_RNASEQ_PATH,
    RAW_METADATA_PATH,
    TRUSTED_LONG_PATH,
    TRUSTED_MATRIX_PATH,
    REFINED_EDA_PATH,
    REFINED_METRICS_PATH,
    REFINED_PREDICTIONS_PATH,
    REFINED_VISUALIZATIONS_PATH
]

for ruta in rutas:
    dbutils.fs.mkdirs(ruta)

print("Estructura creada correctamente:")

for ruta in rutas:
    print(ruta)

# COMMAND ----------

# Validación de consistencia entre tabla long y tabla samples

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