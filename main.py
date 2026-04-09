import logging
from config import SEARCH_TERMS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")


def run_collector():
    from collector.ckan_client import list_packages, collect_all_search_terms

    logger.info("▶ Etapa 1: Coleta de dados")
    packages = list_packages()
    logger.info("  Datasets disponíveis: %d", len(packages))

    results = collect_all_search_terms()
    total = sum(len(v) for v in results.values())
    logger.info("  Datasets coletados por busca: %d", total)

    return results


def run_etl(raw_results):
    logger.info("▶ Etapa 2: ETL — em breve")
    pass


def run_storage(dataframes):
    logger.info("▶ Etapa 3: Storage — em breve")
    pass


def run_analysis():
    logger.info("▶ Etapa 4: Analysis — em breve")
    pass


if __name__ == "__main__":
    print("\n" + "=" * 55)
    print("  SC Data Pipeline — Portal Dados Abertos de SC")
    print("=" * 55 + "\n")

    raw_results = run_collector()
    run_etl(raw_results)
    run_storage(None)
    run_analysis()

    print("\n✅ Pipeline concluído.")
