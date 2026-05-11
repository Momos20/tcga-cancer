from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd


def normalize_name(value: str) -> str:
    value = str(value)
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_")


def find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    if df.empty:
        return None

    normalized = {normalize_name(col): col for col in df.columns}
    normalized_candidates = [normalize_name(c) for c in candidates]

    for candidate in normalized_candidates:
        if candidate in normalized:
            return normalized[candidate]

    for candidate in normalized_candidates:
        for norm_col, original in normalized.items():
            if candidate in norm_col or norm_col in candidate:
                return original

    return None


def numeric_columns(df: pd.DataFrame) -> list[str]:
    cols = []
    for col in df.columns:
        serie = pd.to_numeric(df[col], errors="coerce")
        if serie.notna().sum() > 0:
            cols.append(col)
    return cols


def categorical_columns(df: pd.DataFrame) -> list[str]:
    return [col for col in df.columns if col not in numeric_columns(df)]


def to_numeric_safe(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce")


def dataframe_preview(df: pd.DataFrame, limit: int = 200) -> dict:
    if df.empty:
        return {"columns": [], "rows": []}
    return {
        "columns": list(df.columns),
        "rows": df.head(limit).fillna("").to_dict(orient="records"),
    }


def file_exists(path: Path) -> bool:
    return path.exists() and path.is_file()
