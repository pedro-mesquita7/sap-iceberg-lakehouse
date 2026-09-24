"""Paths and catalog configuration shared by the simulator, extractor, dbt and Dagster."""

from __future__ import annotations

import os
from pathlib import Path

from pyiceberg.catalog import Catalog, load_catalog

REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = REPO_ROOT / "dbt"
CATALOG_NAME = "lakehouse"
BRONZE_NAMESPACE = "bronze"


def lakehouse_home() -> Path:
    """Directory holding all local state: ERP database, Iceberg warehouse, DuckDB file."""
    home = Path(os.environ.get("SAP_LAKEHOUSE_HOME", REPO_ROOT / "lakehouse")).resolve()
    home.mkdir(parents=True, exist_ok=True)
    return home


def erp_db_path() -> Path:
    return lakehouse_home() / "erp.db"


def catalog_properties() -> dict[str, str]:
    """Iceberg SQL catalog backed by SQLite, with data files on the local filesystem.

    The fsspec FileIO is used because PyArrow's local FileIO mishandles Windows drive letters.
    Swapping this for a REST/Glue catalog and an S3 warehouse is a config-only change.
    """
    home = lakehouse_home()
    return {
        "type": "sql",
        "uri": f"sqlite:///{(home / 'iceberg_catalog.db').as_posix()}",
        "warehouse": (home / "warehouse").as_uri(),
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
    }


def get_catalog() -> Catalog:
    return load_catalog(CATALOG_NAME, **catalog_properties())


def dbt_env() -> dict[str, str]:
    """Environment for dbt subprocesses: profiles.yml reads the lakehouse location from here."""
    return {**os.environ, "SAP_LAKEHOUSE_HOME": lakehouse_home().as_posix()}
