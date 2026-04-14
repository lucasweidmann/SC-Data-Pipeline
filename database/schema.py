import sqlite3
import logging
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).parent.parent))
from config import DB_PATH

logger = logging.getLogger(__name__)


CREATE_DATASETS = """
CREATE TABLE IF NOT EXISTS datasets (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    nome        TEXT NOT NULL UNIQUE,
    linhas      INTEGER,
    colunas     INTEGER,
    carregado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_REGISTROS = """
CREATE TABLE IF NOT EXISTS registros (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id  INTEGER NOT NULL,
    dados       TEXT NOT NULL,
    FOREIGN KEY (dataset_id) REFERENCES datasets(id)
);
"""


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def create_tables():
    logger.info("Criando tabelas no banco: %s", DB_PATH)
    with get_connection() as conn:
        conn.execute(CREATE_DATASETS)
        conn.execute(CREATE_REGISTROS)
    logger.info("Tabelas criadas com sucesso.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    create_tables()
