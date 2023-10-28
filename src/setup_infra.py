from pathlib import Path

import duckdb

from db_utils import execute_ddl_stmt


def get_db_dir() -> Path:
    try:
        return Path(__file__).joinpath("..", "db").resolve()
    except Exception as err:
        raise


def setup_metadata_db(metadata_db_path: Path) -> None:
    query = """
    CREATE TABLE metadata.freshness_checks (
        id INTEGER PRIMARY KEY,
        dataset_id TEXT NOT NULL,
        dataset_name TEXT NOT NULL,
        source_data_last_modified TIMESTAMP WITH TIME ZONE NOT NULL,
        local_data_updated BOOLEAN DEFAULT FALSE,
        time_of_metadata_check TIMESTAMP WITH TIME ZONE NOT NULL
    )"""
    execute_ddl_stmt(stmt=query, db_path=metadata_db_path)


def main() -> None:
    db_dir = get_db_dir()
    db_dir.mkdir(exist_ok=True)
    dwh_db_path = db_dir.joinpath("dwh.duckdb")
    metadata_db_path = db_dir.joinpath("dwh_metadata.duckdb")
    setup_metadata_db(metadata_db_path)


if __name__ == "__main__":
    main()
