import logging
import re
import pandas as pd

logger = logging.getLogger(__name__)

_DATE_COLUMN_PATTERNS = re.compile(
    r"(data|date|dt_|_dt|ano|year|mes|month|abertura|encerramento|criacao|atualizacao)",
    re.IGNORECASE,
)

_DATE_FORMATS = [
    "%d/%m/%Y",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y/%m/%d",
]


def _try_parse_dates(series: pd.Series) -> pd.Series:
    for fmt in _DATE_FORMATS:
        try:
            return pd.to_datetime(series, format=fmt, errors="raise")
        except Exception:
            continue
    try:
        return pd.to_datetime(series, infer_datetime_format=True, errors="raise")
    except Exception:
        return series


def convert_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if _DATE_COLUMN_PATTERNS.search(col) and df[col].dtype == object:
            original = df[col].copy()
            df[col] = _try_parse_dates(df[col])
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                logger.info("  Coluna convertida para datetime: '%s'", col)
            else:
                df[col] = original
    return df


def convert_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        sample = df[col].dropna().head(50)
        cleaned = sample.str.replace(r"\.", "", regex=True).str.replace(",", ".", regex=False)
        try:
            pd.to_numeric(cleaned, errors="raise")
            df[col] = (
                df[col]
                .str.replace(r"\.", "", regex=True)
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
            logger.info("  Coluna convertida para numérico: '%s'", col)
        except Exception:
            continue
    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.select_dtypes(include="datetime64").columns:
        df[f"{col}_ano"] = df[col].dt.year
        df[f"{col}_mes"] = df[col].dt.month
        logger.info("  Colunas derivadas criadas: '%s_ano', '%s_mes'", col, col)
    return df


def transform(df: pd.DataFrame, name: str = "dataset") -> pd.DataFrame:
    logger.info("Transformando '%s'", name)
    df = convert_date_columns(df)
    df = convert_numeric_columns(df)
    df = add_derived_columns(df)
    return df
