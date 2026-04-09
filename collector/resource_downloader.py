import time
import logging
from pathlib import Path

import requests
import pandas as pd

import sys
sys.path.append(str(Path(__file__).parent.parent))
from config import RAW_DIR, REQUEST_TIMEOUT, REQUEST_DELAY

logger = logging.getLogger(__name__)


def download_csv_resource(resource: dict) -> pd.DataFrame | None:
    fmt = resource.get("format", "").upper()
    url = resource.get("url", "")
    rid = resource.get("id", "unknown")
    name = resource.get("name", rid)

    if fmt not in ("CSV", ""):
        logger.info("Pulando resource '%s' (formato: %s)", name, fmt)
        return None

    if not url:
        logger.warning("Resource '%s' sem URL.", name)
        return None

    logger.info("Baixando: %s", url)

    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT, stream=True)
        response.raise_for_status()

        safe_name = name[:60].replace(" ", "_").replace("/", "-")
        dest = RAW_DIR / f"{safe_name}_{rid[:8]}.csv"
        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info("Arquivo salvo: %s", dest)
        time.sleep(REQUEST_DELAY)

        df = _read_csv_safe(dest)
        return df

    except requests.exceptions.RequestException as e:
        logger.error("Erro ao baixar '%s': %s", url, e)
        return None


def _read_csv_safe(path: Path) -> pd.DataFrame | None:
    encodings = ["utf-8", "latin-1", "cp1252"]
    separators = [",", ";", "|", "\t"]

    for enc in encodings:
        for sep in separators:
            try:
                df = pd.read_csv(path, encoding=enc, sep=sep, low_memory=False)
                if df.shape[1] > 1:
                    logger.info(
                        "Lido com enc=%s sep='%s' → %d linhas × %d colunas",
                        enc, sep, len(df), df.shape[1],
                    )
                    return df
            except Exception:
                continue

    logger.error("Não foi possível ler o CSV: %s", path)
    return None


def download_package_resources(package: dict) -> dict[str, pd.DataFrame]:
    resources = package.get("resources", [])
    result = {}

    if not resources:
        logger.warning("Package '%s' não tem resources.", package.get("name"))
        return result

    logger.info(
        "Package '%s' tem %d resource(s).",
        package.get("name"), len(resources)
    )

    for resource in resources:
        name = resource.get("name", resource.get("id", "sem_nome"))
        df = download_csv_resource(resource)
        if df is not None:
            result[name] = df

    return result
