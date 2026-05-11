from __future__ import annotations

import pandas as pd

COLUMNAS_REQUERIDAS_NUEVA_MUESTRA = {
    "sample_id",
    "patient_id",
    "gene_id_base",
    "log2_tpm",
}


def validar_archivo_nueva_muestra(df: pd.DataFrame) -> dict:
    columnas = set(df.columns)
    faltantes = sorted(COLUMNAS_REQUERIDAS_NUEVA_MUESTRA - columnas)

    errores = []
    advertencias = []

    if faltantes:
        errores.append(f"Faltan columnas obligatorias: {', '.join(faltantes)}")

    if len(df) == 0:
        errores.append("El archivo está vacío.")

    if "log2_tpm" in df.columns:
        valores_numericos = pd.to_numeric(df["log2_tpm"], errors="coerce")
        n_invalidos = int(valores_numericos.isna().sum())
        if n_invalidos > 0:
            errores.append(f"La columna log2_tpm tiene {n_invalidos} valores no numéricos o nulos.")

    if "sample_id" in df.columns:
        n_muestras = df["sample_id"].nunique()
        if n_muestras > 1:
            advertencias.append(
                f"El archivo contiene {n_muestras} muestras. La inferencia debe procesarse por muestra."
            )

    if "gene_id_base" in df.columns:
        n_genes = df["gene_id_base"].nunique()
        if n_genes < 20:
            advertencias.append(
                f"El archivo contiene solo {n_genes} genes únicos. Para una predicción real debe contener suficientes genes usados por el modelo."
            )

    return {
        "valido": len(errores) == 0,
        "errores": errores,
        "advertencias": advertencias,
        "filas": int(len(df)),
        "columnas": list(df.columns),
        "muestras": int(df["sample_id"].nunique()) if "sample_id" in df.columns else 0,
        "pacientes": int(df["patient_id"].nunique()) if "patient_id" in df.columns else 0,
        "genes": int(df["gene_id_base"].nunique()) if "gene_id_base" in df.columns else 0,
    }
