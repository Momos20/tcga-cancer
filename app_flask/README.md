# App Flask v2 - Clasificación multiclase de cáncer con RNA-Seq

Aplicación web para exponer la zona `refined` del proyecto de clasificación multiclase de tipos de cáncer a partir de expresión génica RNA-Seq.

## Archivos CSV esperados

Ubique estos archivos en la carpeta `data/`:

```text
data/
├── refined_calidad_datos.csv
├── refined_conteo_clases.csv
├── refined_expresion_global.csv
├── refined_resumen_general.csv
├── refined_top_genes_por_clase.csv
└── refined_metricas_modelos_sparkml.csv
```

Estos archivos corresponden a las salidas reales exportadas desde:

```text
/Volumes/workspace/default/tcga_cancer_ml/refined/
```

## Imágenes opcionales

Descargue las imágenes de Databricks desde:

```text
/Volumes/workspace/default/tcga_cancer_ml/refined/visualizations/
```

y póngalas en:

```text
static/img/
```

Nombres esperados:

```text
comparacion_modelos_f1_macro.png
curvas_precision_recall_ovr_mejor_modelo.png
curvas_roc_ovr_mejor_modelo.png
distribucion_clases.png
expresion_promedio_por_clase.png
matriz_confusion_mejor_modelo.png
top20_genes_variables.png
```

## Ejecutar localmente

```bash
cd app_cancer_flask_v2
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python3 app.py
```

En Windows:

```bash
cd app_cancer_flask_v2
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
python app.py
```

Abrir:

```text
http://127.0.0.1:5000
```

## Secciones incluidas

- Home
- Calidad de los datos, con resumen general integrado
- Distribución de clases
- Expresión global
- Top genes por clase
- Comparación de modelos
- Visualizaciones por métricas
- Nueva predicción, con validación de archivo CSV en formato largo
