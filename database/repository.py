import json
import logging

import pandas as pd

from database.schema import get_connection

logger = logging.getLogger(__name__)


def insert_dataset(nome: str, linhas: int, colunas: int) -> int:
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO datasets (nome, linhas, colunas)
            VALUES (?, ?, ?)
            ON CONFLICT(nome) DO UPDATE SET
                linhas = excluded.linhas,
                colunas = excluded.colunas,
                carregado_em = CURRENT_TIMESTAMP
            """,
            (nome, linhas, colunas),
        )
        conn.execute("DELETE FROM registros WHERE dataset_id = (SELECT id FROM datasets WHERE nome = ?)", (nome,))
        return cursor.lastrowid


def insert_dataframe(nome: str, df: pd.DataFrame, batch_size: int = 500):
    logger.info("Inserindo '%s' no banco — %d linhas", nome, len(df))

    dataset_id = insert_dataset(nome, len(df), df.shape[1])

    df_str = df.astype(str)
    rows = [
        (dataset_id, json.dumps(row.to_dict(), ensure_ascii=False))
        for _, row in df_str.iterrows()
    ]

    with get_connection() as conn:
        for i in range(0, len(rows), batch_size):
            conn.executemany(
                "INSERT INTO registros (dataset_id, dados) VALUES (?, ?)",
                rows[i : i + batch_size],
            )

    logger.info("  Inserido com sucesso: %d registros", len(rows))


def list_datasets() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM datasets ORDER BY carregado_em DESC").fetchall()
    return [dict(r) for r in rows]


def fetch_dataset(nome: str) -> pd.DataFrame:
    with get_connection() as conn:
        row = conn.execute("SELECT id FROM datasets WHERE nome = ?", (nome,)).fetchone()
        if not row:
            logger.warning("Dataset '%s' não encontrado no banco.", nome)
            return pd.DataFrame()
        rows = conn.execute(
            "SELECT dados FROM registros WHERE dataset_id = ?", (row["id"],)
        ).fetchall()

    records = [json.loads(r["dados"]) for r in rows]
    return pd.DataFrame(records)
