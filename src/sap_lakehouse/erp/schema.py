"""SAP SD (Order-to-Cash) table definitions, modelled on ECC/S4HANA data dictionary tables.

Values are stored the way SAP hands them to an extractor (RFC_READ_TABLE / ODP): every field is
a character string. That keeps the raw quirks intact for the lakehouse to deal with:

* ALPHA conversion: numeric keys are zero-padded (KUNNR ``0000100001``, MATNR 18 chars)
* Dates are ``YYYYMMDD`` and "no date" is ``00000000``
* Amounts are stored with 2 decimals regardless of currency (see TCURX) and negatives
  carry a trailing minus sign (``125.00-``)
* Units and codes are internal German keys (``ST`` = piece, ``TA`` = standard order)
"""

from __future__ import annotations

from dataclasses import dataclass

# Character length of each key field, used to build and parse SAP-style concatenated TABKEYs.
KEY_FIELD_LENGTHS: dict[str, int] = {
    "MANDT": 3,
    "CURRKEY": 5,
    "KUNNR": 10,
    "MATNR": 18,
    "SPRAS": 1,
    "VBELN": 10,
    "POSNR": 6,
}

CLIENT = "100"  # MANDT: the SAP client every row belongs to


@dataclass(frozen=True)
class SapTable:
    name: str
    description: str
    keys: tuple[str, ...]
    fields: tuple[str, ...]

    def tabkey(self, row: dict[str, str]) -> str:
        """Concatenate key fields at fixed width, like SAP change-log TABKEYs."""
        return "".join(row[k].ljust(KEY_FIELD_LENGTHS[k]) for k in self.keys)

    def parse_tabkey(self, tabkey: str) -> dict[str, str]:
        values, pos = {}, 0
        for k in self.keys:
            width = KEY_FIELD_LENGTHS[k]
            values[k] = tabkey[pos : pos + width].rstrip()
            pos += width
        return values


TABLES: dict[str, SapTable] = {
    t.name: t
    for t in [
        SapTable(
            "TCURX",
            "Currency decimal places (only currencies that deviate from 2)",
            keys=("CURRKEY",),
            fields=("CURRKEY", "CURRDEC"),
        ),
        SapTable(
            "KNA1",
            "Customer master (general data)",
            keys=("MANDT", "KUNNR"),
            fields=("MANDT", "KUNNR", "NAME1", "LAND1", "ORT01", "KTOKD", "ERDAT", "LOEVM"),
        ),
        SapTable(
            "MARA",
            "Material master (general data)",
            keys=("MANDT", "MATNR"),
            fields=(
                "MANDT",
                "MATNR",
                "MTART",
                "MATKL",
                "MEINS",
                "BRGEW",
                "GEWEI",
                "ERSDA",
                "LAEDA",
            ),
        ),
        SapTable(
            "MAKT",
            "Material descriptions (one row per language)",
            keys=("MANDT", "MATNR", "SPRAS"),
            fields=("MANDT", "MATNR", "SPRAS", "MAKTX"),
        ),
        SapTable(
            "VBAK",
            "Sales document header",
            keys=("MANDT", "VBELN"),
            fields=(
                "MANDT",
                "VBELN",
                "ERDAT",
                "ERZET",
                "AUART",
                "VKORG",
                "VTWEG",
                "SPART",
                "KUNNR",
                "NETWR",
                "WAERK",
                "AEDAT",
                "GBSTK",
            ),
        ),
        SapTable(
            "VBAP",
            "Sales document item",
            keys=("MANDT", "VBELN", "POSNR"),
            fields=(
                "MANDT",
                "VBELN",
                "POSNR",
                "MATNR",
                "KWMENG",
                "VRKME",
                "NETWR",
                "WAERK",
                "ABGRU",
                "ERDAT",
                "AEDAT",
            ),
        ),
        SapTable(
            "LIKP",
            "Delivery header",
            keys=("MANDT", "VBELN"),
            fields=("MANDT", "VBELN", "ERDAT", "KUNNR", "WADAT_IST", "AEDAT"),
        ),
        SapTable(
            "LIPS",
            "Delivery item",
            keys=("MANDT", "VBELN", "POSNR"),
            fields=("MANDT", "VBELN", "POSNR", "VGBEL", "VGPOS", "MATNR", "LFIMG", "VRKME"),
        ),
        SapTable(
            "VBRK",
            "Billing document header",
            keys=("MANDT", "VBELN"),
            fields=("MANDT", "VBELN", "FKART", "FKDAT", "KUNRG", "NETWR", "WAERK", "FKSTO"),
        ),
        SapTable(
            "VBRP",
            "Billing document item",
            keys=("MANDT", "VBELN", "POSNR"),
            fields=(
                "MANDT",
                "VBELN",
                "POSNR",
                "VGBEL",
                "VGPOS",
                "AUBEL",
                "AUPOS",
                "MATNR",
                "FKIMG",
                "NETWR",
            ),
        ),
    ]
}

# Delta queue: one row per change, like an SLT logging table or the ODP delta queue.
# Only the key and operation are logged; the extractor reads the current row image.
CHANGE_LOG_DDL = """
CREATE TABLE IF NOT EXISTS ZODP_DELTA_LOG (
    SEQ       INTEGER PRIMARY KEY AUTOINCREMENT,
    TABNAME   TEXT NOT NULL,
    TABKEY    TEXT NOT NULL,
    OP        TEXT NOT NULL CHECK (OP IN ('I', 'U', 'D')),
    LOGGED_AT TEXT NOT NULL
)
"""

SIM_STATE_DDL = "CREATE TABLE IF NOT EXISTS ZSIM_STATE (NAME TEXT PRIMARY KEY, VALUE TEXT NOT NULL)"


def table_ddl(table: SapTable) -> str:
    cols = ", ".join(f"{f} TEXT NOT NULL DEFAULT ''" for f in table.fields)
    return (
        f"CREATE TABLE IF NOT EXISTS {table.name} ({cols}, PRIMARY KEY ({', '.join(table.keys)}))"
    )
