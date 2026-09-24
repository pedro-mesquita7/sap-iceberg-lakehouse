import sqlite3
from contextlib import closing
from datetime import datetime

from sap_lakehouse.erp.schema import CLIENT, TABLES
from sap_lakehouse.erp.simulator import ErpSimulator
from sap_lakehouse.extract.odp import OdpExtractor, current_watermark


def replay_bronze(extractor: OdpExtractor, name: str) -> set[tuple]:
    """Rebuild a table from its bronze CDC events: latest event per key, minus deletes."""
    table = TABLES[name]
    events = extractor.bronze_table(table).scan().to_arrow().to_pylist()
    latest: dict[tuple, dict] = {}
    for event in sorted(events, key=lambda e: e["_seq"]):
        latest[tuple(event[k.lower()] for k in table.keys)] = event
    return {tuple(e[f.lower()] for f in table.fields) for e in latest.values() if e["_op"] != "D"}


def source_rows(simulator: ErpSimulator, name: str) -> set[tuple]:
    with closing(sqlite3.connect(simulator.db_path)) as con:
        return set(con.execute(f"SELECT {', '.join(TABLES[name].fields)} FROM {name}").fetchall())


def test_initial_load_then_rerun_is_a_no_op(
    simulator: ErpSimulator, extractor: OdpExtractor
) -> None:
    first = {r.table: r for r in extractor.extract_all()}
    assert all(r.load_type == "INIT" for r in first.values())
    assert first["VBAK"].inserts == len(source_rows(simulator, "VBAK"))

    second = extractor.extract_all()
    assert all(r.load_type == "NONE" for r in second)


def test_bronze_replays_to_exactly_the_source_state(
    simulator: ErpSimulator, extractor: OdpExtractor
) -> None:
    """The core CDC guarantee: after any sequence of runs, bronze reconstructs every SAP table."""
    extractor.extract_all()
    for _ in range(4):
        simulator.advance(2)
        extractor.extract_all()
    for name in TABLES:
        assert replay_bronze(extractor, name) == source_rows(simulator, name), name


def test_delta_nets_out_inserts_updates_and_deletes(
    simulator: ErpSimulator, extractor: OdpExtractor
) -> None:
    extractor.extract_all()
    with closing(sqlite3.connect(simulator.db_path)) as con:
        kunnr = con.execute("SELECT KUNNR FROM KNA1 WHERE KTOKD = '0001' LIMIT 1").fetchone()[0]
        vbeln = con.execute("SELECT VBELN FROM VBAK LIMIT 1").fetchone()[0]

    sim = simulator
    with closing(sim._connect()) as con, con:
        sim._clock = datetime(2026, 2, 1, 9, 0)
        customer = {"MANDT": CLIENT, "KUNNR": kunnr}
        sim._update(con, "KNA1", customer, {"ORT01": "Faro"})
        sim._update(con, "KNA1", customer, {"ORT01": "Coimbra"})  # two updates -> one event
        new = {"MANDT": CLIENT, "KUNNR": "0000999999", "NAME1": "New Co", "LAND1": "PT"}
        sim._insert(con, "KNA1", new)
        temp = {"MANDT": CLIENT, "KUNNR": "0000888888", "NAME1": "Typo Co", "LAND1": "PT"}
        sim._insert(con, "KNA1", temp)
        sim._delete(con, "KNA1", temp)  # created and deleted between runs -> nothing to send
        sim._delete(con, "VBAK", {"MANDT": CLIENT, "VBELN": vbeln})

    kna1 = extractor.extract_table("KNA1")
    assert (kna1.load_type, kna1.inserts, kna1.updates, kna1.deletes) == ("DELTA", 1, 1, 0)
    vbak = extractor.extract_table("VBAK")
    assert (vbak.inserts, vbak.updates, vbak.deletes) == (0, 0, 1)

    events = extractor.bronze_table(TABLES["KNA1"]).scan().to_arrow().to_pylist()
    update = next(e for e in events if e["kunnr"] == kunnr and e["_op"] == "U")
    assert update["ort01"] == "Coimbra"  # the current image, not the intermediate one
    assert update["_changed_at"] is not None

    deleted = next(
        e
        for e in extractor.bronze_table(TABLES["VBAK"]).scan().to_arrow().to_pylist()
        if e["_op"] == "D"
    )
    assert deleted["vbeln"] == vbeln
    assert deleted["netwr"] is None  # a delete only carries the key


def test_watermark_is_committed_with_the_data(
    simulator: ErpSimulator, extractor: OdpExtractor
) -> None:
    extractor.extract_all()
    simulator.advance(1)
    result = extractor.extract_table("VBAK")
    with closing(sqlite3.connect(simulator.db_path)) as con:
        high = con.execute("SELECT MAX(SEQ) FROM ZODP_DELTA_LOG").fetchone()[0]

    table = extractor.bronze_table(TABLES["VBAK"])
    assert current_watermark(table) == result.watermark == high
    assert table.current_snapshot().summary.additional_properties["sap.odp.load_type"] == "DELTA"
