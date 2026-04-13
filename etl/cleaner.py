import logging
import pandas as pd

logger = logging.getLogger(__name__)


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(r"[^\w]", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )
    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    if removed > 0:
        logger.info("  Duplicatas removidas: %d", removed)
    return df


def strip_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())
    return df


def drop_empty_columns(df: pd.DataFrame, threshold: float = 0.95) -> pd.DataFrame:
    null_ratio = df.isnull().mean()
    cols_to_drop = null_ratio[null_ratio > threshold].index.tolist()
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)
        logger.info("  Colunas vazias removidas (%d%%+ nulos): %s", int(threshold * 100), cols_to_drop)
    return df


def clean(df: pd.DataFrame, name: str = "dataset") -> pd.DataFrame:
    logger.info("Limpando '%s' — %d linhas × %d colunas", name, len(df), df.shape[1])
    df = normalize_column_names(df)
    df = strip_string_columns(df)
    df = remove_duplicates(df)
    df = drop_empty_columns(df)
    logger.info("  Resultado: %d linhas × %d colunas", len(df), df.shape[1])
    return df
