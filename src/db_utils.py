from pathlib import Path

import duckdb
import pandas as pd


def execute_ddl_stmt(stmt: str, db_path: Path) -> None:
    with duckdb.connect(database=str(db_path), read_only=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(stmt)
        conn.commit()


def execute_query(query: str, db_path: Path) -> pd.DataFrame:
    with duckdb.connect(database=str(db_path)) as conn:
        df = conn.execute(query).fetchdf()
    return df


def get_latest_dwh_dataset_freshness(dataset_id: str, db_path: Path) -> pd.DataFrame:
    query = f"""
        SELECT *
        FROM metadata.freshness_checks
        WHERE
            dataset_id = '{dataset_id}'
            AND dwh_data_updated = TRUE
        ORDER BY source_data_last_modified DESC
        LIMIT 1"""
    return execute_query(query=query, db_path=db_path)
