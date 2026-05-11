from __future__ import annotations

from pathlib import Path
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.io as pio
from flask import Flask, render_template, request, redirect, url_for, flash

from services.data_loader import load_table, table_status, get_data_mode, load_all
from services.utils import find_col, numeric_columns, categorical_columns, dataframe_preview, to_numeric_safe
from services.validation import validar_archivo_nueva_muestra

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
IMG_DIR = BASE_DIR / "static" / "img"
UPLOAD_DIR.mkdir(exist_ok=True)

app = Flask(__name__)
app.secret_key = "cancer-rnaseq-demo-secret-v2"


def fig_to_html(fig):
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=30, r=30, t=70, b=40),
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(family="Inter, Arial, sans-serif"),
    )
    return pio.to_html(fig, full_html=False, include_plotlyjs="cdn", config={"displayModeBar": False})


def error_empty(nombre: str):
    return (
        f"No se encontraron datos para {nombre}. "
        "Verifique que el CSV correspondiente esté en la carpeta data/ o que la conexión a Databricks esté configurada."
    )


def image_files():
    expected = [
        ("Comparación de modelos", "comparacion_modelos_f1_macro.png"),
        ("Curvas Precision-Recall OvR", "curvas_precision_recall_ovr_mejor_modelo.png"),
        ("Curvas ROC OvR", "curvas_roc_ovr_mejor_modelo.png"),
        ("Distribución de clases", "distribucion_clases.png"),
        ("Expresión promedio por clase", "expresion_promedio_por_clase.png"),
        ("Matriz de confusión", "matriz_confusion_mejor_modelo.png"),
        ("Top 20 genes variables", "top20_genes_variables.png"),
    ]
    return [
        {
            "titulo": title,
            "filename": filename,
            "exists": (IMG_DIR / filename).exists(),
        }
        for title, filename in expected
    ]


@app.route("/")
def dashboard():
    data = load_all()
    status = table_status()

    total_sources = len(status)
    available_sources = sum(1 for s in status if s["existe"])

    resumen = data.get("resumen_general", pd.DataFrame())
    conteo = data.get("conteo_clases", pd.DataFrame())
    matriz = data.get("matriz_genes", pd.DataFrame())
    metricas = data.get("metricas", pd.DataFrame())

    cards = [
        {
            "label": "Fuentes refined disponibles",
            "value": f"{available_sources}/{total_sources}",
            "hint": "Archivos CSV o tablas conectadas",
        },
        {
            "label": "Clases de cáncer",
            "value": conteo.shape[0] if not conteo.empty else "—",
            "hint": "Tipos presentes en los datos",
        },
        {
            "label": "Genes en matriz ML",
            "value": max(matriz.shape[1] - 3, 0) if not matriz.empty else "—",
            "hint": "Columnas de genes estimadas",
        },
        {
            "label": "Modelos evaluados",
            "value": metricas["modelo"].nunique() if not metricas.empty and "modelo" in metricas.columns else "—",
            "hint": "Métricas SparkML",
        },
    ]

    grafica_clases = None
    if not conteo.empty:
        clase_col = find_col(conteo, ["cancer_type", "clase", "label", "tipo_cancer", "tipo_de_cancer"])
        count_col = find_col(conteo, ["count", "conteo", "n", "total", "n_muestras", "muestras"])
        if clase_col and count_col:
            temp = conteo.copy()
            temp[count_col] = pd.to_numeric(temp[count_col], errors="coerce")
            fig = px.bar(
                temp.sort_values(count_col, ascending=False).head(20),
                x=clase_col,
                y=count_col,
                title="Distribución de clases de cáncer",
                text=count_col,
            )
            fig.update_traces(texttemplate="%{text:.0f}", textposition="outside")
            grafica_clases = fig_to_html(fig)

    grafica_modelos = None
    if not metricas.empty:
        modelo_col = find_col(metricas, ["modelo", "model", "algoritmo"])
        split_col = find_col(metricas, ["split", "particion", "dataset"])
        f1_col = find_col(metricas, ["f1_macro", "macro_f1", "f1"])
        if modelo_col and f1_col:
            temp = metricas.copy()
            temp[f1_col] = pd.to_numeric(temp[f1_col], errors="coerce")
            if split_col:
                temp = temp[temp[split_col].astype(str).str.lower().isin(["test", "validation", "validacion", "val"])]
                fig = px.bar(
                    temp,
                    x=modelo_col,
                    y=f1_col,
                    color=split_col,
                    barmode="group",
                    title="Comparación de modelos según F1 macro",
                    text=f1_col,
                )
            else:
                fig = px.bar(
                    temp,
                    x=modelo_col,
                    y=f1_col,
                    title="Comparación de modelos según F1 macro",
                    text=f1_col,
                )
            fig.update_traces(texttemplate="%{text:.3f}", textposition="outside")
            grafica_modelos = fig_to_html(fig)

    return render_template(
        "dashboard.html",
        cards=cards,
        status=status,
        data_mode=get_data_mode(),
        grafica_clases=grafica_clases,
        grafica_modelos=grafica_modelos,
        images=image_files(),
    )


@app.route("/resumen")
def resumen():
    return redirect(url_for("calidad"))


@app.route("/calidad")
def calidad():
    df = load_table("calidad_datos")
    resumen_df = load_table("resumen_general")
    resumen_preview = dataframe_preview(resumen_df, limit=300) if not resumen_df.empty else {"columns": [], "rows": []}
    if df.empty:
        return render_template(
            "calidad.html",
            error=error_empty("refined_calidad_datos"),
            resumen_columns=resumen_preview["columns"],
            resumen_rows=resumen_preview["rows"],
        )

    variable_col = find_col(df, ["variable", "columna", "campo", "feature"])
    null_col = find_col(df, ["nulos", "nulls", "missing", "porcentaje_nulos", "pct_nulos", "missing_pct"])

    grafica = None
    if variable_col and null_col:
        temp = df.copy()
        temp[null_col] = pd.to_numeric(temp[null_col], errors="coerce")
        temp = temp.sort_values(null_col, ascending=False).head(25)
        fig = px.bar(
            temp,
            x=null_col,
            y=variable_col,
            orientation="h",
            title="Variables con mayor proporción o cantidad de valores faltantes",
            text=null_col,
        )
        grafica = fig_to_html(fig)

    preview = dataframe_preview(df, limit=300)
    return render_template(
        "calidad.html",
        grafica=grafica,
        columns=preview["columns"],
        rows=preview["rows"],
        resumen_columns=resumen_preview["columns"],
        resumen_rows=resumen_preview["rows"],
    )


@app.route("/clases")
def clases():
    df = load_table("conteo_clases")
    if df.empty:
        return render_template("clases.html", error=error_empty("refined_conteo_clases"))

    clase_col = find_col(df, ["cancer_type", "clase", "tipo_cancer", "tipo_de_cancer", "label"])
    count_col = find_col(df, ["count", "conteo", "n", "total", "n_muestras", "muestras"])

    grafica = None
    if clase_col and count_col:
        temp = df.copy()
        temp[count_col] = pd.to_numeric(temp[count_col], errors="coerce")
        fig = px.bar(
            temp.sort_values(count_col, ascending=False),
            x=clase_col,
            y=count_col,
            title="Distribución de muestras por tipo de cáncer",
            text=count_col,
        )
        fig.update_traces(texttemplate="%{text:.0f}", textposition="outside")
        grafica = fig_to_html(fig)

    preview = dataframe_preview(df, limit=300)
    return render_template(
        "clases.html",
        grafica=grafica,
        columns=preview["columns"],
        rows=preview["rows"],
    )


@app.route("/expresion")
def expresion():
    df = load_table("expresion_global")
    if df.empty:
        return render_template("expresion.html", error=error_empty("refined_expresion_global"))

    cat_cols = categorical_columns(df)
    num_cols = numeric_columns(df)

    grafica = None
    if cat_cols and num_cols:
        x_col = cat_cols[0]
        y_col = num_cols[0]
        temp = df.copy()
        temp[y_col] = pd.to_numeric(temp[y_col], errors="coerce")
        temp = temp.sort_values(y_col, ascending=False).head(30)

        fig = px.bar(
            temp,
            x=x_col,
            y=y_col,
            title=f"Expresión global: {y_col} por {x_col}",
            text=y_col,
        )
        grafica = fig_to_html(fig)

    preview = dataframe_preview(df, limit=300)
    return render_template(
        "expresion.html",
        grafica=grafica,
        columns=preview["columns"],
        rows=preview["rows"],
    )


@app.route("/genes")
def genes():
    df = load_table("top_genes")
    if df.empty:
        return render_template("genes.html", error=error_empty("refined_top_genes_por_clase"))

    clase_col = find_col(df, ["cancer_type", "clase", "tipo_cancer", "tipo_de_cancer", "label"])
    gene_col = find_col(df, ["gene_id_base", "gene", "gen", "gene_id", "symbol", "gene_symbol"])
    metric_col = find_col(df, ["varianza", "variance", "mean", "promedio", "importancia", "score", "log2_tpm", "expresion_promedio"])

    selected_class = request.args.get("clase", "").strip()
    temp = df.copy()

    clases = []
    if clase_col:
        clases = sorted([str(x) for x in temp[clase_col].dropna().unique()])
        if selected_class:
            temp = temp[temp[clase_col].astype(str) == selected_class]

    grafica = None
    if gene_col and metric_col:
        temp_plot = temp.copy()
        temp_plot[metric_col] = pd.to_numeric(temp_plot[metric_col], errors="coerce")
        temp_plot = temp_plot.sort_values(metric_col, ascending=False).head(25)
        fig = px.bar(
            temp_plot,
            x=metric_col,
            y=gene_col,
            orientation="h",
            title="Top genes por clase",
            text=metric_col,
        )
        grafica = fig_to_html(fig)

    preview = dataframe_preview(temp, limit=300)
    return render_template(
        "genes.html",
        grafica=grafica,
        columns=preview["columns"],
        rows=preview["rows"],
        clases=clases,
        selected_class=selected_class,
    )


@app.route("/modelos")
def modelos():
    df = load_table("metricas")
    if df.empty:
        return render_template("modelos.html", error=error_empty("refined_metricas_modelos_sparkml"))

    modelo_col = find_col(df, ["modelo", "model", "algoritmo"])
    split_col = find_col(df, ["split", "particion", "dataset"])
    f1_col = find_col(df, ["f1_macro", "macro_f1", "f1"])
    acc_col = find_col(df, ["accuracy", "exactitud"])
    bal_col = find_col(df, ["balanced_accuracy", "accuracy_balanceado"])

    grafica_f1 = None
    grafica_acc = None

    if modelo_col and f1_col:
        temp = df.copy()
        temp[f1_col] = pd.to_numeric(temp[f1_col], errors="coerce")
        if split_col:
            fig = px.bar(
                temp,
                x=modelo_col,
                y=f1_col,
                color=split_col,
                barmode="group",
                title="F1 macro por modelo y partición",
                text=f1_col,
            )
        else:
            fig = px.bar(
                temp,
                x=modelo_col,
                y=f1_col,
                title="F1 macro por modelo",
                text=f1_col,
            )
        fig.update_traces(texttemplate="%{text:.3f}", textposition="outside")
        grafica_f1 = fig_to_html(fig)

    if modelo_col and acc_col:
        temp = df.copy()
        temp[acc_col] = pd.to_numeric(temp[acc_col], errors="coerce")
        metric_y = bal_col if bal_col else acc_col
        if bal_col:
            temp[bal_col] = pd.to_numeric(temp[bal_col], errors="coerce")
        if split_col:
            fig = px.bar(
                temp,
                x=modelo_col,
                y=metric_y,
                color=split_col,
                barmode="group",
                title=f"{metric_y} por modelo y partición",
                text=metric_y,
            )
        else:
            fig = px.bar(
                temp,
                x=modelo_col,
                y=metric_y,
                title=f"{metric_y} por modelo",
                text=metric_y,
            )
        fig.update_traces(texttemplate="%{text:.3f}", textposition="outside")
        grafica_acc = fig_to_html(fig)

    preview = dataframe_preview(df, limit=300)
    return render_template(
        "modelos.html",
        grafica_f1=grafica_f1,
        grafica_acc=grafica_acc,
        columns=preview["columns"],
        rows=preview["rows"],
    )


@app.route("/visualizaciones")
def visualizaciones():
    return render_template("visualizaciones.html", images=image_files())


@app.route("/nueva-prediccion", methods=["GET", "POST"])
def nueva_prediccion():
    if request.method == "GET":
        return render_template("nueva_prediccion.html")

    file = request.files.get("archivo")
    if not file or file.filename == "":
        flash("Debe seleccionar un archivo CSV.", "error")
        return redirect(url_for("nueva_prediccion"))

    if not file.filename.lower().endswith(".csv"):
        flash("El archivo debe ser CSV.", "error")
        return redirect(url_for("nueva_prediccion"))

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = f"{timestamp}_{file.filename.replace(' ', '_')}"
    upload_path = UPLOAD_DIR / safe_name
    file.save(upload_path)

    try:
        df = pd.read_csv(upload_path)
    except Exception as exc:
        return render_template(
            "resultado_prediccion.html",
            validacion={
                "valido": False,
                "errores": [f"No fue posible leer el CSV: {exc}"],
                "advertencias": [],
                "filas": 0,
                "columnas": [],
                "muestras": 0,
                "pacientes": 0,
                "genes": 0,
            },
            archivo=str(upload_path),
            columns=[],
            rows=[],
        )

    validacion = validar_archivo_nueva_muestra(df)
    preview = dataframe_preview(df, limit=30)

    return render_template(
        "resultado_prediccion.html",
        validacion=validacion,
        archivo=str(upload_path),
        columns=preview["columns"],
        rows=preview["rows"],
    )


if __name__ == "__main__":
    app.run(debug=True)
