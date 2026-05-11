CREATE TABLE IF NOT EXISTS workspace.default.refined_calidad_datos
USING DELTA
LOCATION '/Volumes/workspace/default/tcga_cancer_ml/refined/eda_outputs/calidad_datos';

CREATE TABLE IF NOT EXISTS workspace.default.refined_conteo_clases
USING DELTA
LOCATION '/Volumes/workspace/default/tcga_cancer_ml/refined/eda_outputs/conteo_clases';

CREATE TABLE IF NOT EXISTS workspace.default.refined_expresion_global
USING DELTA
LOCATION '/Volumes/workspace/default/tcga_cancer_ml/refined/eda_outputs/expresion_global';

CREATE TABLE IF NOT EXISTS workspace.default.refined_resumen_general
USING DELTA
LOCATION '/Volumes/workspace/default/tcga_cancer_ml/refined/eda_outputs/resumen_general';

CREATE TABLE IF NOT EXISTS workspace.default.refined_top_genes_por_clase
USING DELTA
LOCATION '/Volumes/workspace/default/tcga_cancer_ml/refined/eda_outputs/top_genes_por_clase';

CREATE TABLE IF NOT EXISTS workspace.default.refined_matriz_1000_genes
USING DELTA
LOCATION '/Volumes/workspace/default/tcga_cancer_ml/refined/ml_matrix/matriz_1000_genes';

CREATE TABLE IF NOT EXISTS workspace.default.refined_metricas_modelos_sparkml
USING DELTA
LOCATION '/Volumes/workspace/default/tcga_cancer_ml/refined/model_metrics/metricas_modelos_sparkml';
