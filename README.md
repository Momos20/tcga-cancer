# TCGA Cancer ML

Incluye dos componentes:

1. **Pipeline reproducible en Databricks**
   - ingesta
   - preparación
   - EDA
   - modelado SparkML
   - capa de aplicación sobre resultados refined

2. **Aplicación Flask**
   - visualización de resultados exportados desde `refined`
   - consulta de métricas, clases, expresión global y genes
   - flujo de inferencia con nuevas muestras

---

## 1. Estructura del repositorio

```text
tcga_cancer_repo/
├── databricks/
│   ├── notebooks/
│   │   ├── 00_configuracion.ipynb
│   │   ├── 01_descarga_ingesta_gdc_raw.ipynb
│   │   ├── 02_preparacion_trusted.ipynb
│   │   ├── 03_eda_sparksql.ipynb
│   │   ├── 04_modelo_sparkml_multiclase.ipynb
│   │   └── 05_aplicacion_visualizacion_refined.ipynb
│   └── scripts/
│       ├── 00_configuracion.py
│       ├── 01_descarga_ingesta_gdc_raw.py
│       ├── 02_preparacion_trusted.py
│       ├── 03_eda_sparksql.py
│       └── 04_modelo_sparkml_multiclase.py
├── app_flask/
│   ├── app.py
│   ├── requirements.txt
│   ├── .env.example
│   ├── databricks/
│   ├── data/
│   ├── services/
│   ├── static/
│   ├── templates/
│   └── scripts/
├── docs/
├── evidences/
├── .gitignore
└── README.md
```

---

## 2. Qué contiene cada parte

### `databricks/`
Contiene el pipeline principal del proyecto, listo para reproducirse por etapas dentro de Databricks.

#### Orden de ejecución recomendado
1. `00_configuracion.ipynb`
2. `01_descarga_ingesta_gdc_raw.ipynb`
3. `02_preparacion_trusted.ipynb`
4. `03_eda_sparksql.ipynb`
5. `04_modelo_sparkml_multiclase.ipynb`
6. `05_aplicacion_visualizacion_refined.ipynb`

### `app_flask/`
Contiene la aplicación web construida adicionalmente para exponer parte de los resultados del proyecto. Esta app consume archivos exportados desde la zona `refined` del datalake.

---

## 3. Reproducibilidad del pipeline en Databricks

### Requisitos
- Workspace de Databricks con acceso a Volumes
- Unity Catalog habilitado
- acceso a internet para consultar/descargar desde GDC
- permisos para crear tablas en `workspace.default`

### Flujo batch implementado
- **raw**: archivos originales y metadatos de ingesta
- **trusted**: datos limpios, filtrados y validados
- **refined**: salidas EDA, métricas, predicciones y visualizaciones
- **models**: artefactos del mejor modelo SparkML

### Tablas principales generadas
#### Raw
- `workspace.default.raw_tcga_metadatos_completo`
- `workspace.default.raw_tcga_metadatos_oficial_18_clases`
- `workspace.default.raw_tcga_manifest_descargas`

#### Trusted
- `workspace.default.trusted_tcga_rnaseq_long_18_clases`
- `workspace.default.trusted_tcga_samples_18_clases`
- `workspace.default.trusted_tcga_gene_dictionary`

#### Refined
- `workspace.default.refined_eda_resumen_general`
- `workspace.default.refined_eda_conteo_clases`
- `workspace.default.refined_eda_desbalance_clases`
- `workspace.default.refined_eda_tipos_muestra`
- `workspace.default.refined_eda_calidad_datos`
- `workspace.default.refined_eda_genes_detectados_muestra`
- `workspace.default.refined_eda_expresion_global`
- `workspace.default.refined_eda_top_genes_por_clase`
- `workspace.default.refined_eda_genes_mas_variables`
- `workspace.default.refined_eda_muestras_por_paciente`
- `workspace.default.refined_ml_matriz_100_genes`
- `workspace.default.refined_metricas_modelos_sparkml`
- `workspace.default.refined_reporte_clasificacion_por_clase`
- `workspace.default.refined_predicciones_test_mejor_modelo`
- `workspace.default.refined_matriz_confusion_mejor_modelo`

---

## 4. Cómo ejecutar la aplicación Flask

Entre a la carpeta:

```bash
cd app_flask
```

Cree el entorno virtual e instale dependencias:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Copie variables de entorno:

```bash
cp .env.example .env
```

Ejecute:

```bash
python app.py
```

Abrir en navegador:

```text
http://127.0.0.1:5000
```

---

## 5. Relación entre Databricks y la aplicación

La aplicación Flask no reemplaza el pipeline batch.  
La lógica correcta del repositorio es:

1. ejecutar el pipeline en Databricks;
2. generar tablas y salidas en `refined`;
3. exportar los CSV necesarios para la app;
4. ejecutar la app localmente o desplegarla.

Dentro de `app_flask/databricks/` ya existen utilidades para exportar resultados y registrar tablas.

---


