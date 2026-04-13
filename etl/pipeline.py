import json
import logging
from pathlib import Path

import pandas as pd

import sys
sys.path.append(str(Path(__file__).parent.parent))
from config import RAW_DIR, PROCESSED_DIR
from etl.cleaner import clean
from etl.transformer import transform
from collector.resource_downloader import download_csv_resource

logger = logging.getLogger(__name__)


def load_search_results() -> list[dict]:
    path = RAW_DIR / "all_searches.json"
    if not path.exists():
        logger.error("Arquivo não encontrado: %s — rode o collector primeiro.", path)
        return []
    with open(path, encoding="utf-8") as f:
        all_searches = json.load(f)
    seen = set()
    datasets = []
    for term_results in all_searches.values():
        for dataset in term_results:
            did = dataset.get("id")
            if did and did not in seen:
                seen.add(did)
                datasets.append(dataset)
    logger.info("Datasets únicos carregados para ETL: %d", len(datasets))
    return datasets


def save_processed(df: pd.DataFrame, name: str) -> Path:
    safe_name = name[:60].replace(" ", "_").replace("/", "-")
    path = PROCESSED_DIR / f"{safe_name}.csv"
    df.to_csv(path, index=False, encoding="utf-8")
    logger.info("Processado salvo: %s", path)
    return path


def run_etl(datasets: list[dict] | None = None) -> dict[str, pd.DataFrame]:
    if datasets is None:
        datasets = load_search_results()
    if not datasets:
        logger.warning("Nenhum dataset para processar.")
        return {}
    processed = {}
    for dataset in datasets:
        name = dataset.get("name", dataset.get("id", "desconhecido"))
        resources = dataset.get("resources", [])
        if not resources:
            continue
        for resource in resources:
            fmt = resource.get("format", "").upper()
            if fmt not in ("CSV", ""):
                continue
            df_raw = download_csv_resource(resource)
            if df_raw is None or df_raw.empty:
                continue
            df_clean = clean(df_raw, name=name)
            df_final = transform(df_clean, name=name)
            save_processed(df_final, name)
            processed[name] = df_final
            break
    logger.info("ETL concluído. Datasets processados: %d", len(processed))
    return processed


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    result = run_etl()
    print(f"\nDatasets prontos para análise: {len(result)}")
    for name, df in result.items():
        print(f"  {name}: {len(df)} linhas × {df.shape[1]} colunas")
