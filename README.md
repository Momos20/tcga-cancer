# TCGA Cancer ML

## Descripción general

Este repositorio integra dos componentes principales del proyecto:

1. **Pipeline batch reproducible en Databricks**
   - Ingesta de datos desde el **Genomic Data Commons (GDC)**.
   - Organización del datalake en las zonas **raw**, **trusted**, **refined** y **models**.
   - Preparación de datos con **PySpark**.
   - Análisis exploratorio de datos con **SparkSQL**.
   - Entrenamiento y evaluación de modelos de clasificación multiclase con **SparkML**.
   - Persistencia de tablas analíticas, métricas, predicciones y modelo final.

2. **Aplicación web en Flask**
   - Visualización de resultados analíticos.
   - Exploración de métricas y salidas del modelo.
   - Capa de consumo orientada a mostrar el valor del pipeline de datos de forma más accesible.

---

## Estructura del repositorio

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

## Flujo batch implementado en Databricks

El pipeline del proyecto sigue una lógica secuencial y reproducible:

1. **Configuración del entorno y definición de rutas**
2. **Ingesta de datos desde GDC hacia raw**
3. **Preparación y limpieza en trusted**
4. **Catalogación de tablas en Unity Catalog**
5. **EDA con SparkSQL y persistencia en refined**
6. **Modelado multiclase con SparkML**
7. **Persistencia del mejor modelo en models**
8. **Consumo de resultados desde aplicación o capa visual**

### Zonas del datalake

| Zona | Propósito |
|---|---|
| `raw` | Archivos originales descargados desde GDC y metadatos de ingesta |
| `trusted` | Datos limpios, filtrados, validados y listos para análisis/modelado |
| `refined` | Tablas de EDA, métricas, predicciones, reportes y visualizaciones |
| `models` | Artefactos persistidos del mejor modelo SparkML |

---

## Orden de ejecución recomendado en Databricks

Los notebooks deben ejecutarse en este orden:

1. `00_configuracion.ipynb`
2. `01_descarga_ingesta_gdc_raw.ipynb`
3. `02_preparacion_trusted.ipynb`
4. `03_eda_sparksql.ipynb`
5. `04_modelo_sparkml_multiclase.ipynb`
6. `05_aplicacion_visualizacion_refined.ipynb`

Este orden garantiza que cada etapa consuma las salidas generadas por la etapa anterior.

---

## Descripción breve de cada notebook

### `00_configuracion.ipynb`
Define rutas del proyecto, estructura del datalake, clases oficiales y variables de configuración general.

### `01_descarga_ingesta_gdc_raw.ipynb`
Consulta el API del GDC, construye metadatos, filtra los proyectos seleccionados y descarga archivos RNA-Seq a la zona `raw`.

### `02_preparacion_trusted.ipynb`
Realiza la limpieza, transformación y validación de los datos con PySpark, y persiste tablas confiables en `trusted`.

### `03_eda_sparksql.ipynb`
Ejecuta el análisis exploratorio sobre tablas catalogadas, genera tablas analíticas y visualizaciones persistidas en `refined`.

### `04_modelo_sparkml_multiclase.ipynb`
Construye la matriz de modelado, divide entrenamiento/validación/prueba, entrena modelos SparkML, compara métricas y guarda el mejor modelo.

### `05_aplicacion_visualizacion_refined.ipynb`
Consume resultados desde `refined` para construir una capa mínima de aplicación y visualización.

---

## Aplicación Flask

Además del pipeline reproducible en Databricks, el proyecto incluye una **aplicación web en Flask** como componente adicional de exposición de resultados.

### Propósito de la app
- presentar resultados del proyecto de forma más amigable,
- explorar salidas analíticas y métricas,
- complementar la parte técnica del pipeline con una interfaz de consumo.

### Rol dentro del proyecto
La app no reemplaza el pipeline batch, sino que funciona como **capa de aplicación** construida sobre los resultados generados por Databricks.

---

## Datos del proyecto

Los datos provienen del **Genomic Data Commons (GDC)**, específicamente de archivos abiertos asociados a proyectos **TCGA**, filtrados con los siguientes criterios:

- `data_category = 'Transcriptome Profiling'`
- `data_type = 'Gene Expression Quantification'`
- `workflow_type = 'STAR - Counts'`

El pipeline trabaja con una selección oficial de **18 clases de cáncer** y filtra posteriormente las muestras de tipo **Primary Tumor**.

---

## Tecnologías utilizadas

- **Databricks**
- **Databricks Volumes**
- **Unity Catalog**
- **PySpark**
- **SparkSQL**
- **SparkML**
- **Python**
- **Flask**
- **Pandas**
- **Matplotlib**

---

## Resultados esperados del repositorio

Este repositorio permite evidenciar:

- una arquitectura batch implementada de extremo a extremo,
- un pipeline reproducible para ingesta, preparación, EDA y modelado,
- persistencia de resultados en distintas zonas del datalake,
- y una aplicación complementaria para consumo de resultados.


