from datetime import date
from pathlib import Path

import pytest

from sap_lakehouse.config import erp_db_path, get_catalog
from sap_lakehouse.erp.simulator import ErpSimulator
from sap_lakehouse.extract.odp import OdpExtractor

START = date(2026, 1, 5)  # a Monday


@pytest.fixture
def lakehouse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / "lakehouse"
    monkeypatch.setenv("SAP_LAKEHOUSE_HOME", str(home))
    return home


@pytest.fixture
def simulator(lakehouse: Path) -> ErpSimulator:
    sim = ErpSimulator(erp_db_path(), seed=7)
    sim.initialize(START, history_days=10)
    return sim


@pytest.fixture
def extractor(simulator: ErpSimulator) -> OdpExtractor:
    return OdpExtractor(erp_db_path(), get_catalog())
