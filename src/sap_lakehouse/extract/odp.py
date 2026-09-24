"""Delta extraction from SAP into Iceberg bronze tables, following ODP/SLT semantics.

How a run works for one table:

1. Read the table's watermark (last change-log sequence already landed) from the current Iceberg
   snapshot. No snapshot yet means an initial load.
2. Read the global high-water mark of the change log *before* reading any data. Anything that
   changes after that point is picked up again next run, which is safe because bronze is an
   append-only event log and silver keeps the latest event per key.
3. Initial load: copy every current row. Delta: collapse the log entries per key in the window
   (watermark, high] and read the current row image. A missing row becomes a delete; a row that
   was created and deleted inside the window is skipped (net no-op).
4. Append the events and the new watermark in a single Iceberg commit, so data and progress can
   never drift apart. A crash before the commit simply means the window is re-read.
"""

from __future__ import annotations

import sqlite3
import uuid
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import LongType, NestedField, StringType, TimestampType, TimestamptzType

from sap_lakehouse.config import BRONZE_NAMESPACE
from sap_lakehouse.erp.schema import TABLES, SapTable

WATERMARK_PROPERTY = "sap.odp.watermark"


@dataclass(frozen=True)
class ExtractResult:
    table: str
    load_type: str  # INIT, DELTA or NONE (nothing new)
    inserts: int
    updates: int
    deletes: int
    watermark: int | None
    snapshot_id: int | None

    @property
    def rows(self) -> int:
        return self.inserts + self.updates + self.deletes


def bronze_identifier(table: SapTable) -> str:
    return f"{BRONZE_NAMESPACE}.{table.name.lower()}"


def bronze_schema(table: SapTable) -> Schema:
    """SAP fields stay raw strings in bronze; CDC metadata columns are typed."""
    fields = [
        NestedField(i, name.lower(), StringType(), required=False)
        for i, name in enumerate(table.fields, start=1)
    ]
    metadata = [
        ("_op", StringType(), "I/U/D as delivered by the delta queue"),
        ("_seq", LongType(), "Change-log sequence of the last change included in this event"),
        (
            "_changed_at",
            TimestampType(),
            "Source-system time of that change (null on initial load)",
        ),
        ("_load_type", StringType(), "INIT or DELTA"),
        ("_batch_id", StringType(), "Extraction run identifier"),
        ("_extracted_at", TimestamptzType(), "When the event landed in the lakehouse"),
    ]
    for offset, (name, field_type, doc) in enumerate(metadata, start=len(fields) + 1):
        fields.append(NestedField(offset, name, field_type, required=False, doc=doc))
    return Schema(*fields)


def current_watermark(table: Table) -> int | None:
    snapshot = table.current_snapshot()
    if snapshot is None:
        return None
    return int(snapshot.summary.additional_properties[WATERMARK_PROPERTY])


def new_batch_id() -> str:
    return f"{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"


class OdpExtractor:
    def __init__(self, erp_db: Path, catalog: Catalog) -> None:
        self.erp_db = erp_db
        self.catalog = catalog

    def extract_all(self) -> list[ExtractResult]:
        batch_id = new_batch_id()
        return [self.extract_table(name, batch_id) for name in TABLES]

    def extract_table(self, name: str, batch_id: str | None = None) -> ExtractResult:
        sap_table = TABLES[name.upper()]
        iceberg_table = self.bronze_table(sap_table)
        watermark = current_watermark(iceberg_table)
        batch_id = batch_id or new_batch_id()

        with closing(sqlite3.connect(self.erp_db)) as con:
            con.row_factory = sqlite3.Row
            high = con.execute("SELECT COALESCE(MAX(SEQ), 0) FROM ZODP_DELTA_LOG").fetchone()[0]
            if watermark is None:
                load_type = "INIT"
                events = self._initial_load(con, sap_table, high)
            else:
                load_type = "DELTA"
                events = self._delta(con, sap_table, watermark, high)

        if not events:
            # An empty initial load writes nothing, so the next run simply initializes again.
            return ExtractResult(name, "NONE", 0, 0, 0, watermark, None)

        extracted_at = datetime.now(UTC)
        rows = [
            {
                **{f.lower(): values.get(f) for f in sap_table.fields},
                "_op": op,
                "_seq": seq,
                "_changed_at": changed_at,
                "_load_type": load_type,
                "_batch_id": batch_id,
                "_extracted_at": extracted_at,
            }
            for op, seq, changed_at, values in events
        ]
        iceberg_table.append(
            pa.Table.from_pylist(rows, schema=iceberg_table.schema().as_arrow()),
            snapshot_properties={
                WATERMARK_PROPERTY: str(high),
                "sap.odp.load_type": load_type,
                "sap.odp.batch_id": batch_id,
            },
        )
        ops = [e[0] for e in events]
        return ExtractResult(
            table=name,
            load_type=load_type,
            inserts=ops.count("I"),
            updates=ops.count("U"),
            deletes=ops.count("D"),
            watermark=high,
            snapshot_id=iceberg_table.current_snapshot().snapshot_id,
        )

    def bronze_table(self, sap_table: SapTable) -> Table:
        self.catalog.create_namespace_if_not_exists(BRONZE_NAMESPACE)
        schema = bronze_schema(sap_table)
        spec = PartitionSpec(
            PartitionField(
                source_id=schema.find_field("_extracted_at").field_id,
                field_id=1000,
                transform=DayTransform(),
                name="_extracted_day",
            )
        )
        return self.catalog.create_table_if_not_exists(
            bronze_identifier(sap_table),
            schema=schema,
            partition_spec=spec,
            properties={
                "comment": f"SAP {sap_table.name}: {sap_table.description} (CDC events)",
                "sap.table": sap_table.name,
                "sap.keys": ",".join(k.lower() for k in sap_table.keys),
            },
        )

    @staticmethod
    def _initial_load(
        con: sqlite3.Connection, sap_table: SapTable, high: int
    ) -> list[tuple[str, int, datetime | None, dict[str, str | None]]]:
        rows = con.execute(f"SELECT * FROM {sap_table.name}").fetchall()
        return [("I", high, None, dict(row)) for row in rows]

    @staticmethod
    def _delta(
        con: sqlite3.Connection, sap_table: SapTable, low: int, high: int
    ) -> list[tuple[str, int, datetime | None, dict[str, str | None]]]:
        log = con.execute(
            "SELECT TABKEY, OP, SEQ, LOGGED_AT FROM ZODP_DELTA_LOG "
            "WHERE TABNAME = ? AND SEQ > ? AND SEQ <= ? ORDER BY SEQ",
            (sap_table.name, low, high),
        ).fetchall()

        # Net change per key: remember the first operation and the last sequence in the window
        changes: dict[str, tuple[str, int, str]] = {}
        for tabkey, op, seq, logged_at in log:
            first_op = changes[tabkey][0] if tabkey in changes else op
            changes[tabkey] = (first_op, seq, logged_at)

        where = " AND ".join(f"{k} = ?" for k in sap_table.keys)
        events = []
        for tabkey, (first_op, seq, logged_at) in sorted(changes.items(), key=lambda c: c[1][1]):
            key = sap_table.parse_tabkey(tabkey)
            row = con.execute(
                f"SELECT * FROM {sap_table.name} WHERE {where}", [key[k] for k in sap_table.keys]
            ).fetchone()
            changed_at = datetime.fromisoformat(logged_at)
            if row is not None:
                events.append(("I" if first_op == "I" else "U", seq, changed_at, dict(row)))
            elif first_op != "I":
                events.append(("D", seq, changed_at, dict(key)))
        return events
