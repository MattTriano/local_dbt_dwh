from pathlib import Path

import duckdb

from db_utils import execute_ddl_stmt


def get_project_root_dir() -> Path:
    try:
        return Path(__file__).joinpath("..", "..").resolve()
    except Exception as err:
        raise


def get_db_dir() -> Path:
    try:
        return Path(__file__).joinpath("..", "..", "db").resolve()
    except Exception as err:
        raise


def setup_metadata_db(metadata_db_path: Path) -> None:
    execute_ddl_stmt(stmt="CREATE SCHEMA IF NOT EXISTS metadata", db_path=metadata_db_path)
    execute_ddl_stmt(stmt="CREATE SEQUENCE serial START 1", db_path=metadata_db_path)
    query = """
    CREATE TABLE metadata.freshness_checks (
        id INTEGER PRIMARY KEY DEFAULT NEXTVAL('serial'),
        dataset_id TEXT NOT NULL,
        dataset_name TEXT NOT NULL,
        source_data_last_modified TIMESTAMP WITH TIME ZONE NOT NULL,
        dwh_data_updated BOOLEAN DEFAULT FALSE,
        time_of_metadata_check TIMESTAMP WITH TIME ZONE NOT NULL
    );
    """
    execute_ddl_stmt(stmt=query, db_path=metadata_db_path)


def main() -> None:
    proj_root_dir = get_project_root_dir()
    db_dir = proj_root_dir.joinpath("db")
    db_dir.mkdir(exist_ok=True)
    data_dir = proj_root_dir.joinpath("data")
    data_dir.mkdir(exist_ok=True)
    dwh_db_path = db_dir.joinpath("dwh.duckdb")
    metadata_db_path = db_dir.joinpath("dwh_metadata.duckdb")
    setup_metadata_db(metadata_db_path)


if __name__ == "__main__":
    main()
