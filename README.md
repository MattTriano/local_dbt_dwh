# DBT Data Warehouse

This project is uses `dbt`, `duckdb`, and a little bit of python code to collect, extract, load, and transform some data sets, curating a local data warehouse.

## Setup

Do the basics:
* Clone and `cd` into the repo
* Create and activate the conda env (`conda env create -f environment.yml` and then `conda activate elt_env`)

Then call `python src/setup_infra.py` to create the necessary directory structure and initialize the `metadata` database.

## Data Collection

```python
from src.data_connector import SocrataTable, SocrataTableMetadata

intake = SocrataTable(table_id="3k7z-hchi", table_name="ccsao_intake")
intake_metadata = SocrataTableMetadata(socrata_table=intake)
intake_metadate.refresh_data()
```

## Steps Taken

```bash
dbt init dbt_dwh
```
And select the `duckdb` option.

`cd` into `/dbt_dwh/` then modify the `dwh_project.yml` file:
* add `docs-paths: ["docs"]`
* in the `dbt_dwh:` section of `models:`, add `data_raw: -> +materialized: table` and `clean: +materialized: table`

Add a `dependencies.yml` file, add [dbt_utils](https://hub.getdbt.com/dbt-labs/dbt_utils/latest) and any other packages with version numbers, then run `dbt deps`

Add `/data_raw/` and `/clean/` dirs to the `/models/` dir, and then add `.sql` files to `/data_raw/` with the format:
* file_name: `dataset_name_raw.sql`
* file contents: `select * from read_csv("../data/dataset_name.csv", AUTO_DETECT=TRUE)`

Then run `dbt run`
