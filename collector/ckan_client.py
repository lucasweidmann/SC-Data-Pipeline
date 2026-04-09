import json
import time
import logging
from pathlib import Path

import urllib3
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import sys
sys.path.append(str(Path(__file__).parent.parent))
from config import (
    CKAN_ENDPOINTS,
    RAW_DIR,
    REQUEST_TIMEOUT,
    REQUEST_DELAY,
    MAX_RETRIES,
    SEARCH_TERMS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

def _get(url: str, params: dict = None) -> dict | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT, verify=False)
            response.raise_for_status()
            data = response.json()

            if not data.get("success"):
                logger.warning("API retornou success=false para %s", url)
                return None

            return data

        except requests.exceptions.Timeout:
            logger.warning("Timeout na tentativa %d/%d: %s", attempt, MAX_RETRIES, url)
        except requests.exceptions.HTTPError as e:
            logger.error("Erro HTTP %s: %s", e.response.status_code, url)
            return None
        except requests.exceptions.RequestException as e:
            logger.warning("Erro na tentativa %d/%d: %s", attempt, MAX_RETRIES, e)

        if attempt < MAX_RETRIES:
            time.sleep(REQUEST_DELAY * attempt)

    logger.error("Falhou após %d tentativas: %s", MAX_RETRIES, url)
    return None


def _save_json(data: dict | list, filename: str) -> Path:
    path = RAW_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("Salvo: %s", path)
    return path

def list_packages() -> list[str]:
    logger.info("Buscando lista de datasets...")
    data = _get(CKAN_ENDPOINTS["package_list"])

    if data is None:
        return []

    packages = data.get("result", [])
    logger.info("Total de datasets encontrados: %d", len(packages))

    _save_json(packages, "package_list.json")
    return packages


def search_packages(term: str, rows: int = 20) -> list[dict]:
    logger.info("Buscando datasets com termo: '%s'", term)

    params = {
        "q": term,
        "rows": rows,
        "start": 0,
    }

    data = _get(CKAN_ENDPOINTS["package_search"], params=params)

    if data is None:
        return []

    results = data.get("result", {}).get("results", [])
    logger.info("  → %d resultado(s) para '%s'", len(results), term)

    safe_term = term.replace(" ", "_").replace("/", "-")
    _save_json(results, f"search_{safe_term}.json")

    time.sleep(REQUEST_DELAY)
    return results


def get_package_details(package_id: str) -> dict | None:
    logger.info("Buscando detalhes do dataset: %s", package_id)

    data = _get(CKAN_ENDPOINTS["package_show"], params={"id": package_id})

    if data is None:
        return None

    result = data.get("result", {})
    _save_json(result, f"package_{package_id[:50]}.json")
    time.sleep(REQUEST_DELAY)
    return result


def collect_all_search_terms() -> dict[str, list[dict]]:
    all_results = {}

    for term in SEARCH_TERMS:
        results = search_packages(term)
        all_results[term] = results

    _save_json(all_results, "all_searches.json")
    logger.info("Coleta concluída. Termos pesquisados: %d", len(SEARCH_TERMS))

    return all_results


if __name__ == "__main__":
    print("=" * 50)
    print("SC Data Pipeline — Collector")
    print("=" * 50)

    packages = list_packages()
    print(f"\nTotal de datasets no portal: {len(packages)}")
    print("Primeiros 10:", packages[:10])

    print("\nBuscando por termos relevantes...")
    results = collect_all_search_terms()

    for term, datasets in results.items():
        print(f"  '{term}': {len(datasets)} dataset(s)")
