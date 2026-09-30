PY      ?= python
DBT      = dbt --no-use-colors
DBT_ARGS = --project-dir dbt_project --profiles-dir dbt_project
export DUCKDB_PATH ?= warehouse.duckdb

.PHONY: help install extract load transform docs lineage sync sync-live pipeline \
        fixture ci test lint

help:  ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-11s %s\n", $$1, $$2}'

install:  ## install pinned dependencies
	$(PY) -m pip install -r requirements.txt

extract:  ## pull GitHub API -> raw JSON -> Parquet (needs GITHUB_READ_TOKEN)
	$(PY) -m extract.github_extract

load:  ## load raw Parquet into DuckDB raw.* tables, untransformed
	$(PY) -m load.load_to_duckdb

transform:  ## dbt build: run all models and tests
	$(DBT) build $(DBT_ARGS)

docs:  ## generate and serve dbt docs (lineage graph)
	$(DBT) docs generate $(DBT_ARGS) && $(DBT) docs serve $(DBT_ARGS)

lineage:  ## regenerate the Mermaid lineage diagram in docs/lineage.md from the dbt manifest
	$(DBT) parse $(DBT_ARGS) && $(PY) -m scripts.render_lineage

sync:  ## reverse ETL, dry run (prints ClickUp tasks, no API calls)
	$(PY) -m reverse_etl.sync_stale_issues

sync-live:  ## reverse ETL, live (CLICKUP_LIST_ID only, needs CLICKUP_API_KEY)
	$(PY) -m reverse_etl.sync_stale_issues --live

pipeline: extract load transform sync  ## full loop, sync in dry-run mode

fixture:  ## build the synthetic fixture warehouse (no network)
	$(PY) -m scripts.generate_fixture
	$(PY) -m extract.to_parquet --raw-json data/sample/json --raw-parquet data/sample/parquet
	$(PY) -m load.load_to_duckdb --raw-parquet data/sample/parquet

ci: export DUCKDB_PATH = ci.duckdb
ci: fixture transform  ## what CI runs: fixture -> dbt build -> dry-run sync
	$(PY) -m reverse_etl.sync_stale_issues > /dev/null

test:  ## python unit tests
	$(PY) -m pytest -q

lint:  ## ruff
	$(PY) -m ruff check .
