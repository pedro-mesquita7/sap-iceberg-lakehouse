import sqlite3
from contextlib import closing
from decimal import Decimal
from pathlib import Path

import pytest

from sap_lakehouse.erp.schema import TABLES
from sap_lakehouse.erp.simulator import ErpSimulator, from_sap_amount, to_sap_amount

from .conftest import START


def dump(db: Path) -> dict[str, list[tuple]]:
    with closing(sqlite3.connect(db)) as con:
        return {t: sorted(con.execute(f"SELECT * FROM {t}").fetchall()) for t in TABLES}


@pytest.mark.parametrize(
    ("value", "currency", "internal"),
    [
        (Decimal("99.90"), "EUR", "99.90"),
        (Decimal("15000"), "JPY", "150.00"),  # JPY has no decimals: SAP stores 1/100
        (Decimal("12.345"), "KWD", "123.45"),  # KWD has 3 decimals: SAP stores x10
        (Decimal("-125.00"), "EUR", "125.00-"),  # trailing minus sign
    ],
)
def test_sap_amount_round_trip(value: Decimal, currency: str, internal: str) -> None:
    assert to_sap_amount(value, currency) == internal
    assert from_sap_amount(internal, currency) == value


def test_same_data_regardless_of_how_days_are_batched(tmp_path: Path) -> None:
    in_one_go = ErpSimulator(tmp_path / "a.db", seed=1)
    in_one_go.initialize(START, history_days=8)

    day_by_day = ErpSimulator(tmp_path / "b.db", seed=1)
    day_by_day.initialize(START, history_days=5)
    day_by_day.advance(1)
    day_by_day.advance(2)

    assert dump(tmp_path / "a.db") == dump(tmp_path / "b.db")


def test_every_row_has_its_insert_logged(simulator: ErpSimulator) -> None:
    with closing(sqlite3.connect(simulator.db_path)) as con:
        for name, table in TABLES.items():
            rows = con.execute(f"SELECT {', '.join(table.keys)} FROM {name}").fetchall()
            logged = {
                key
                for (key,) in con.execute(
                    "SELECT TABKEY FROM ZODP_DELTA_LOG WHERE TABNAME = ? AND OP = 'I'", (name,)
                )
            }
            for row in rows:
                assert table.tabkey(dict(zip(table.keys, row, strict=True))) in logged


def test_document_flow_is_consistent(simulator: ErpSimulator) -> None:
    """Deliveries reference existing orders and invoices reference existing deliveries."""
    with closing(sqlite3.connect(simulator.db_path)) as con:
        orphan_deliveries = con.execute(
            "SELECT COUNT(*) FROM LIPS WHERE VGBEL NOT IN (SELECT VBELN FROM VBAK)"
        ).fetchone()[0]
        orphan_invoices = con.execute(
            "SELECT COUNT(*) FROM VBRP r JOIN VBRK k USING (MANDT, VBELN) "
            "WHERE k.FKART = 'F2' AND r.VGBEL NOT IN (SELECT VBELN FROM LIKP)"
        ).fetchone()[0]
    assert orphan_deliveries == 0
    assert orphan_invoices == 0
