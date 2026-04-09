from pathlib import Path

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
DB_DIR = BASE_DIR / "database"

for _dir in [RAW_DIR, PROCESSED_DIR, DB_DIR]:
    _dir.mkdir(parents=True, exist_ok=True)

CKAN_BASE_URL = "https://dados.sc.gov.br/api/3/action"

CKAN_ENDPOINTS = {
    "package_list":   f"{CKAN_BASE_URL}/package_list",
    "package_search": f"{CKAN_BASE_URL}/package_search",
    "package_show":   f"{CKAN_BASE_URL}/package_show",
    "resource_show":  f"{CKAN_BASE_URL}/resource_show",
}

SEARCH_TERMS = [
    "segurança pública",
    "ouvidoria",
    "transparência",
    "saúde",
    "educação",
]

DB_PATH = DB_DIR / "sc_data.db"

REQUEST_TIMEOUT = 30
REQUEST_DELAY = 0.5
MAX_RETRIES = 3
