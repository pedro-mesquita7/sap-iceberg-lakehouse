"""A small SAP SD system in SQLite that generates realistic Order-to-Cash activity.

Every business day it creates orders, changes and rejects items, deletes a few orders, ships
deliveries, posts goods issue, runs billing and issues the odd credit memo. Every insert, update
and delete is written to ZODP_DELTA_LOG, which is what the extractor reads - the same pattern as
an SLT logging table or the ODP delta queue in a real system.

Randomness is seeded per calendar day, so simulating 5 days in one call or in five calls produces
exactly the same data.
"""

from __future__ import annotations

import random
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from sap_lakehouse.erp.schema import CHANGE_LOG_DDL, CLIENT, SIM_STATE_DDL, TABLES, table_ddl

NO_DATE = "00000000"

# TCURX only lists currencies that do NOT have 2 decimals - everything else defaults to 2.
CURRENCY_DECIMALS = {"JPY": 0, "KWD": 3}
EUR_FX = {
    "EUR": Decimal("1"),
    "USD": Decimal("1.10"),
    "GBP": Decimal("0.86"),
    "JPY": Decimal("160"),
    "KWD": Decimal("0.34"),
}

# country -> (currency, sales org, cities, legal suffix, weight)
COUNTRIES = {
    "PT": ("EUR", "1000", ["Porto", "Lisboa", "Braga", "Aveiro"], "Lda.", 5),
    "ES": ("EUR", "1000", ["Madrid", "Barcelona", "Valencia"], "S.L.", 3),
    "DE": ("EUR", "2000", ["Stuttgart", "München", "Hamburg", "Köln"], "GmbH", 4),
    "FR": ("EUR", "2000", ["Lyon", "Paris", "Toulouse"], "SARL", 2),
    "GB": ("GBP", "2000", ["Manchester", "Leeds", "Bristol"], "Ltd", 2),
    "US": ("USD", "3000", ["Chicago", "Detroit", "Houston"], "Inc.", 2),
    "JP": ("JPY", "3000", ["Osaka", "Nagoya", "Yokohama"], "K.K.", 1),
    "KW": ("KWD", "3000", ["Kuwait City"], "W.L.L.", 1),
}
NAME_A = [
    "Atlantic",
    "Northern",
    "Iberian",
    "Alpine",
    "Coastal",
    "Summit",
    "Harbor",
    "Granite",
    "Riverside",
    "Pioneer",
    "Vertex",
    "Meridian",
]
NAME_B = [
    "Tools",
    "Industrial",
    "Machining",
    "Engineering",
    "Supply",
    "Automation",
    "Components",
    "Fabrication",
    "Hydraulics",
    "Systems",
]

# (MATNR or None for internal numbering, EN, DE, PT, material group, base unit, EUR price, kg)
PRODUCTS = [
    (
        None,
        "Cordless drill 18V",
        "Akku-Bohrschrauber 18V",
        "Berbequim sem fio 18V",
        "TOOLS",
        "ST",
        "129.00",
        "1.600",
    ),
    (
        None,
        "Angle grinder 125mm",
        "Winkelschleifer 125mm",
        "Rebarbadora 125mm",
        "TOOLS",
        "ST",
        "89.00",
        "2.100",
    ),
    (
        None,
        "Impact wrench 1/2in",
        "Schlagschrauber 1/2 Zoll",
        "Chave de impacto 1/2pol",
        "TOOLS",
        "ST",
        "219.00",
        "2.800",
    ),
    (
        None,
        "Battery pack 18V 5Ah",
        "Akkupack 18V 5Ah",
        "Bateria 18V 5Ah",
        "TOOLS",
        "ST",
        "99.00",
        "0.700",
    ),
    (
        None,
        "Laser distance meter",
        "Laser-Entfernungsmesser",
        "Medidor laser de distância",
        "MEASURE",
        "ST",
        "74.00",
        "0.200",
    ),
    (
        None,
        "Rotary laser level",
        "Rotationslaser",
        "Nível laser rotativo",
        "MEASURE",
        "ST",
        "649.00",
        "1.900",
    ),
    (
        None,
        "Pressure sensor 0-10 bar",
        "Drucksensor 0-10 bar",
        "Sensor de pressão 0-10 bar",
        "SENSORS",
        "ST",
        "38.50",
        "0.100",
    ),
    (
        None,
        "Temperature sensor PT100",
        "Temperatursensor PT100",
        "Sensor de temperatura PT100",
        "SENSORS",
        "ST",
        "24.90",
        "0.050",
    ),
    (
        None,
        "Proximity sensor M12",
        "Näherungsschalter M12",
        "Sensor de proximidade M12",
        "SENSORS",
        "ST",
        "31.00",
        "0.080",
    ),
    (
        "HYD-PUMP-5KW",
        "Hydraulic pump 5kW",
        "Hydraulikpumpe 5kW",
        "Bomba hidráulica 5kW",
        "HYDRAULIC",
        "ST",
        "1450.00",
        "38.000",
    ),
    (
        "VALVE-DN25",
        "Control valve DN25",
        "Regelventil DN25",
        "Válvula de controlo DN25",
        "HYDRAULIC",
        "ST",
        "310.00",
        "4.200",
    ),
    (
        None,
        "Hydraulic hose 1m, carton of 10",
        "Hydraulikschlauch 1m, Karton à 10",
        "Mangueira hidráulica 1m, caixa de 10",
        "HYDRAULIC",
        "KAR",
        "96.00",
        "6.000",
    ),
    (
        None,
        "Drill bit set HSS 25pc",
        "HSS-Bohrer-Set 25-tlg.",
        "Conjunto de brocas HSS 25pç",
        "ACCESS",
        "ST",
        "29.90",
        "0.900",
    ),
    (
        None,
        "Saw blade 190mm",
        "Sägeblatt 190mm",
        "Lâmina de serra 190mm",
        "ACCESS",
        "ST",
        "19.50",
        "0.400",
    ),
    (
        None,
        "Cutting disc 125mm, carton of 50",
        "Trennscheibe 125mm, Karton à 50",
        "Disco de corte 125mm, caixa de 50",
        "ACCESS",
        "KAR",
        "42.00",
        "5.500",
    ),
    (
        None,
        "Industrial lubricant",
        "Industrieschmierstoff",
        "Lubrificante industrial",
        "CONSUM",
        "KG",
        "7.80",
        "1.000",
    ),
]
REJECTION_REASONS = ["01", "03", "04"]  # delivery too late, too expensive, competitor better


def sap_date(d: date) -> str:
    return d.strftime("%Y%m%d")


def parse_sap_date(value: str) -> date | None:
    return None if value in ("", NO_DATE) else datetime.strptime(value, "%Y%m%d").date()


def to_sap_amount(value: Decimal, currency: str) -> str:
    """External amount -> SAP internal CURR representation (2 decimals, trailing minus)."""
    decimals = CURRENCY_DECIMALS.get(currency, 2)
    internal = (value * Decimal(10) ** (decimals - 2)).quantize(Decimal("0.01"), ROUND_HALF_UP)
    text = f"{abs(internal):.2f}"
    return f"{text}-" if internal < 0 else text


def from_sap_amount(value: str, currency: str) -> Decimal:
    """SAP internal CURR representation -> external amount."""
    sign = -1 if value.endswith("-") else 1
    internal = Decimal(value.rstrip("-") or "0")
    return sign * internal * Decimal(10) ** (2 - CURRENCY_DECIMALS.get(currency, 2))


def price_in(currency: str, eur_price: Decimal) -> Decimal:
    decimals = CURRENCY_DECIMALS.get(currency, 2)
    return (eur_price * EUR_FX[currency]).quantize(Decimal(10) ** -decimals, ROUND_HALF_UP)


@dataclass
class DaySummary:
    day: date
    counts: dict[str, int] = field(default_factory=dict)

    def bump(self, key: str, n: int = 1) -> None:
        if n:
            self.counts[key] = self.counts.get(key, 0) + n


class ErpSimulator:
    def __init__(self, db_path: Path, seed: int = 42) -> None:
        self.db_path = db_path
        self.seed = seed
        self._clock = datetime.min

    # ------------------------------------------------------------------ public API
    def initialize(self, start: date, history_days: int = 30) -> list[DaySummary]:
        """Create a fresh ERP with master data, then simulate `history_days` of activity."""
        if self.db_path.exists():
            self.db_path.unlink()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as con, con:
            for table in TABLES.values():
                con.execute(table_ddl(table))
            con.execute(CHANGE_LOG_DDL)
            con.execute(SIM_STATE_DDL)
            self._clock = datetime.combine(start, time(6, 0))
            self._create_master_data(con, random.Random(f"{self.seed}:master"), start)
            self._set_state(con, "CURRENT_DATE", start.isoformat())
        return self.advance(history_days)

    def advance(self, days: int = 1) -> list[DaySummary]:
        """Simulate the next `days` business days. Each day commits as one transaction."""
        summaries = []
        for _ in range(days):
            with closing(self._connect()) as con, con:
                day = date.fromisoformat(self._get_state(con, "CURRENT_DATE"))
                summaries.append(self._simulate_day(con, day))
                self._set_state(con, "CURRENT_DATE", (day + timedelta(days=1)).isoformat())
        return summaries

    def current_date(self) -> date:
        with closing(self._connect()) as con:
            return date.fromisoformat(self._get_state(con, "CURRENT_DATE"))

    # ------------------------------------------------------------------ simulation
    def _simulate_day(self, con: sqlite3.Connection, day: date) -> DaySummary:
        rng = random.Random(f"{self.seed}:{day.isoformat()}")
        summary = DaySummary(day)
        self._clock = datetime.combine(day, time(7, 0))
        weekday = day.weekday() < 5

        # Master data maintenance
        if rng.random() < 0.15:
            self._new_customer(con, rng, day)
            summary.bump("customers_created")
        if rng.random() < 0.20 and self._relocate_customer(con, rng):
            summary.bump("customers_relocated")
        if rng.random() < 0.05 and self._rename_material(con, rng, day):
            summary.bump("materials_renamed")

        # Warehouse and billing run on weekdays; nightly billing picks up yesterday's goods issue
        if weekday:
            summary.bump("goods_issues_posted", self._post_goods_issue(con, rng, day))
            summary.bump("invoices_created", self._run_billing(con, day))
            if rng.random() < 0.15 and self._create_credit_memo(con, rng, day):
                summary.bump("credit_memos_created")
            summary.bump("deliveries_created", self._create_deliveries(con, day))

        # Sales activity
        for _ in range(rng.randint(6, 12) if weekday else rng.randint(0, 2)):
            self._create_order(con, rng, day)
            summary.bump("orders_created")
        for _ in range(rng.randint(0, 3)):
            if self._change_quantity(con, rng, day):
                summary.bump("order_items_changed")
        if rng.random() < 0.35 and self._reject_item(con, rng, day):
            summary.bump("order_items_rejected")
        if rng.random() < 0.25 and self._delete_order(con, rng, day):
            summary.bump("orders_deleted")
        return summary

    def _create_master_data(self, con: sqlite3.Connection, rng: random.Random, start: date) -> None:
        for currency, decimals in CURRENCY_DECIMALS.items():
            self._insert(con, "TCURX", {"CURRKEY": currency, "CURRDEC": str(decimals)})
        for _ in range(40):
            self._new_customer(con, rng, start - timedelta(days=rng.randint(60, 2000)))
        # Intercompany customers have external numbers - ALPHA conversion must skip them
        for kunnr, name, land, city in [
            ("IC-NORTH", "Group Company North", "PT", "Braga"),
            ("IC-SOUTH", "Group Company South", "ES", "Madrid"),
        ]:
            self._insert(
                con,
                "KNA1",
                {
                    "MANDT": CLIENT,
                    "KUNNR": kunnr,
                    "NAME1": name,
                    "LAND1": land,
                    "ORT01": city,
                    "KTOKD": "0005",
                    "ERDAT": sap_date(start - timedelta(days=3000)),
                },
            )
        for i, (matnr, en, de, pt, matkl, meins, price, weight) in enumerate(PRODUCTS, start=1):
            matnr = matnr or f"{100000 + i:018d}"
            self._insert(
                con,
                "MARA",
                {
                    "MANDT": CLIENT,
                    "MATNR": matnr,
                    "MTART": "HAWA" if matkl in ("ACCESS", "CONSUM") else "FERT",
                    "MATKL": matkl,
                    "MEINS": meins,
                    "BRGEW": weight,
                    "GEWEI": "KG",
                    "ERSDA": sap_date(start - timedelta(days=rng.randint(200, 3000))),
                    "LAEDA": NO_DATE,
                },
            )
            for spras, text in (("E", en), ("D", de), ("P", pt)):
                self._insert(
                    con, "MAKT", {"MANDT": CLIENT, "MATNR": matnr, "SPRAS": spras, "MAKTX": text}
                )
            self._set_state(con, f"PRICE:{matnr}", price)

    def _new_customer(self, con: sqlite3.Connection, rng: random.Random, day: date) -> None:
        countries = list(COUNTRIES)
        land = rng.choices(countries, weights=[COUNTRIES[c][4] for c in countries])[0]
        _, _, cities, suffix, _ = COUNTRIES[land]
        self._insert(
            con,
            "KNA1",
            {
                "MANDT": CLIENT,
                "KUNNR": f"{self._next_number(con, 'NR_CUSTOMER', 100000):010d}",
                "NAME1": f"{rng.choice(NAME_A)} {rng.choice(NAME_B)} {suffix}",
                "LAND1": land,
                "ORT01": rng.choice(cities),
                "KTOKD": "0001",
                "ERDAT": sap_date(day),
            },
        )

    def _relocate_customer(self, con: sqlite3.Connection, rng: random.Random) -> bool:
        customers = self._rows(con, "SELECT * FROM KNA1 WHERE KTOKD = '0001' ORDER BY KUNNR")
        customer = rng.choice(customers)
        cities = [c for c in COUNTRIES[customer["LAND1"]][2] if c != customer["ORT01"]]
        if not cities:
            return False
        self._update(con, "KNA1", customer, {"ORT01": rng.choice(cities)})
        return True

    def _rename_material(self, con: sqlite3.Connection, rng: random.Random, day: date) -> bool:
        texts = self._rows(
            con,
            "SELECT * FROM MAKT WHERE SPRAS = 'E' AND MAKTX NOT LIKE '% - Gen 2' ORDER BY MATNR",
        )
        if not texts:
            return False
        text = rng.choice(texts)
        self._update(con, "MAKT", text, {"MAKTX": f"{text['MAKTX']} - Gen 2"})
        self._update(
            con, "MARA", {"MANDT": CLIENT, "MATNR": text["MATNR"]}, {"LAEDA": sap_date(day)}
        )
        return True

    def _create_order(self, con: sqlite3.Connection, rng: random.Random, day: date) -> None:
        customer = rng.choice(self._rows(con, "SELECT * FROM KNA1 WHERE LOEVM = '' ORDER BY KUNNR"))
        currency, vkorg, *_ = COUNTRIES[customer["LAND1"]]
        vbeln = f"{self._next_number(con, 'NR_ORDER', 10000000):010d}"
        materials = rng.sample(
            self._rows(con, "SELECT MATNR, MEINS FROM MARA ORDER BY MATNR"), rng.randint(1, 4)
        )
        created_at = self._tick()
        total = Decimal(0)
        for i, material in enumerate(materials, start=1):
            qty = Decimal(rng.randint(1, 40))
            value = qty * price_in(
                currency, Decimal(self._get_state(con, f"PRICE:{material['MATNR']}"))
            )
            total += value
            self._insert(
                con,
                "VBAP",
                {
                    "MANDT": CLIENT,
                    "VBELN": vbeln,
                    "POSNR": f"{i * 10:06d}",
                    "MATNR": material["MATNR"],
                    "KWMENG": f"{qty:.3f}",
                    "VRKME": material["MEINS"],
                    "NETWR": to_sap_amount(value, currency),
                    "WAERK": currency,
                    "ERDAT": sap_date(day),
                    "AEDAT": NO_DATE,
                },
            )
        self._insert(
            con,
            "VBAK",
            {
                "MANDT": CLIENT,
                "VBELN": vbeln,
                "ERDAT": sap_date(day),
                "ERZET": created_at.strftime("%H%M%S"),
                "AUART": "SO" if rng.random() < 0.1 else "TA",
                "VKORG": vkorg,
                "VTWEG": "10",
                "SPART": "00",
                "KUNNR": customer["KUNNR"],
                "NETWR": to_sap_amount(total, currency),
                "WAERK": currency,
                "AEDAT": NO_DATE,
                "GBSTK": "A",
            },
        )

    def _open_orders(self, con: sqlite3.Connection) -> list[dict[str, str]]:
        return self._rows(con, "SELECT * FROM VBAK WHERE GBSTK = 'A' ORDER BY VBELN")

    def _change_quantity(self, con: sqlite3.Connection, rng: random.Random, day: date) -> bool:
        orders = self._open_orders(con)
        if not orders:
            return False
        order = rng.choice(orders)
        items = self._rows(
            con, "SELECT * FROM VBAP WHERE VBELN = ? AND ABGRU = '' ORDER BY POSNR", order["VBELN"]
        )
        if not items:
            return False
        item = rng.choice(items)
        currency = item["WAERK"]
        old_qty = Decimal(item["KWMENG"])
        new_qty = max(Decimal(1), old_qty + rng.choice([-5, -2, 3, 5, 10]))
        unit_price = from_sap_amount(item["NETWR"], currency) / old_qty
        new_value = (unit_price * new_qty).quantize(
            Decimal(10) ** -CURRENCY_DECIMALS.get(currency, 2)
        )
        self._update(
            con,
            "VBAP",
            item,
            {
                "KWMENG": f"{new_qty:.3f}",
                "NETWR": to_sap_amount(new_value, currency),
                "AEDAT": sap_date(day),
            },
        )
        self._refresh_order_value(con, order, day)
        return True

    def _reject_item(self, con: sqlite3.Connection, rng: random.Random, day: date) -> bool:
        orders = self._open_orders(con)
        if not orders:
            return False
        order = rng.choice(orders)
        items = self._rows(con, "SELECT * FROM VBAP WHERE VBELN = ? AND ABGRU = ''", order["VBELN"])
        if not items:
            return False
        self._update(
            con,
            "VBAP",
            rng.choice(items),
            {"ABGRU": rng.choice(REJECTION_REASONS), "AEDAT": sap_date(day)},
        )
        self._refresh_order_value(con, order, day)
        return True

    def _refresh_order_value(
        self, con: sqlite3.Connection, order: dict[str, str], day: date
    ) -> None:
        """Header net value only counts items that are not rejected, like SAP does."""
        currency = order["WAERK"]
        items = self._rows(con, "SELECT * FROM VBAP WHERE VBELN = ?", order["VBELN"])
        total = sum(
            (from_sap_amount(i["NETWR"], currency) for i in items if not i["ABGRU"]), Decimal(0)
        )
        changes = {"NETWR": to_sap_amount(total, currency), "AEDAT": sap_date(day)}
        if all(i["ABGRU"] for i in items):
            changes["GBSTK"] = "C"  # fully rejected orders are complete
        self._update(con, "VBAK", order, changes)

    def _delete_order(self, con: sqlite3.Connection, rng: random.Random, day: date) -> bool:
        """Mistaken orders get deleted outright - the extractor must propagate the delete."""
        candidates = self._rows(
            con,
            "SELECT * FROM VBAK WHERE GBSTK = 'A' AND ERDAT >= ? "
            "AND VBELN NOT IN (SELECT VGBEL FROM LIPS) ORDER BY VBELN",
            sap_date(day - timedelta(days=1)),
        )
        if not candidates:
            return False
        order = rng.choice(candidates)
        for item in self._rows(con, "SELECT * FROM VBAP WHERE VBELN = ?", order["VBELN"]):
            self._delete(con, "VBAP", item)
        self._delete(con, "VBAK", order)
        return True

    def _create_deliveries(self, con: sqlite3.Connection, day: date) -> int:
        created = 0
        for order in self._open_orders(con):
            lead_time = 1 + int(order["VBELN"]) % 4
            if parse_sap_date(order["ERDAT"]) + timedelta(days=lead_time) > day:
                continue
            items = self._rows(
                con, "SELECT * FROM VBAP WHERE VBELN = ? AND ABGRU = ''", order["VBELN"]
            )
            if not items:
                continue
            delivery = f"{self._next_number(con, 'NR_DELIVERY', 80000000):010d}"
            self._insert(
                con,
                "LIKP",
                {
                    "MANDT": CLIENT,
                    "VBELN": delivery,
                    "ERDAT": sap_date(day),
                    "KUNNR": order["KUNNR"],
                    "WADAT_IST": NO_DATE,
                    "AEDAT": NO_DATE,
                },
            )
            for item in items:
                self._insert(
                    con,
                    "LIPS",
                    {
                        "MANDT": CLIENT,
                        "VBELN": delivery,
                        "POSNR": item["POSNR"],
                        "VGBEL": order["VBELN"],
                        "VGPOS": item["POSNR"],
                        "MATNR": item["MATNR"],
                        "LFIMG": item["KWMENG"],
                        "VRKME": item["VRKME"],
                    },
                )
            self._update(con, "VBAK", order, {"GBSTK": "B"})
            created += 1
        return created

    def _post_goods_issue(self, con: sqlite3.Connection, rng: random.Random, day: date) -> int:
        pending = self._rows(
            con,
            "SELECT * FROM LIKP WHERE WADAT_IST = ? AND ERDAT < ? ORDER BY VBELN",
            NO_DATE,
            sap_date(day),
        )
        posted = 0
        for delivery in pending:
            if rng.random() < 0.85:
                self._update(
                    con, "LIKP", delivery, {"WADAT_IST": sap_date(day), "AEDAT": sap_date(day)}
                )
                posted += 1
        return posted

    def _run_billing(self, con: sqlite3.Connection, day: date) -> int:
        """Nightly billing run: invoice every delivery whose goods issue was posted before today."""
        due = self._rows(
            con,
            "SELECT * FROM LIKP WHERE WADAT_IST <> ? AND WADAT_IST < ? "
            "AND VBELN NOT IN (SELECT VGBEL FROM VBRP) ORDER BY VBELN",
            NO_DATE,
            sap_date(day),
        )
        for delivery in due:
            items = self._rows(con, "SELECT * FROM LIPS WHERE VBELN = ?", delivery["VBELN"])
            order = self._rows(con, "SELECT * FROM VBAK WHERE VBELN = ?", items[0]["VGBEL"])[0]
            currency = order["WAERK"]
            invoice = f"{self._next_number(con, 'NR_BILLING', 90000000):010d}"
            total = Decimal(0)
            for item in items:
                order_item = self._rows(
                    con,
                    "SELECT * FROM VBAP WHERE VBELN = ? AND POSNR = ?",
                    item["VGBEL"],
                    item["VGPOS"],
                )[0]
                total += from_sap_amount(order_item["NETWR"], currency)
                self._insert(
                    con,
                    "VBRP",
                    {
                        "MANDT": CLIENT,
                        "VBELN": invoice,
                        "POSNR": item["POSNR"],
                        "VGBEL": delivery["VBELN"],
                        "VGPOS": item["POSNR"],
                        "AUBEL": item["VGBEL"],
                        "AUPOS": item["VGPOS"],
                        "MATNR": item["MATNR"],
                        "FKIMG": item["LFIMG"],
                        "NETWR": order_item["NETWR"],
                    },
                )
            self._insert(
                con,
                "VBRK",
                {
                    "MANDT": CLIENT,
                    "VBELN": invoice,
                    "FKART": "F2",
                    "FKDAT": sap_date(day),
                    "KUNRG": delivery["KUNNR"],
                    "NETWR": to_sap_amount(total, currency),
                    "WAERK": currency,
                },
            )
            self._update(con, "VBAK", order, {"GBSTK": "C"})
        return len(due)

    def _create_credit_memo(self, con: sqlite3.Connection, rng: random.Random, day: date) -> bool:
        """Partial credit for a recent invoice: negative amounts arrive as '123.45-'."""
        invoices = self._rows(
            con,
            "SELECT * FROM VBRK WHERE FKART = 'F2' AND FKDAT >= ? ORDER BY VBELN",
            sap_date(day - timedelta(days=30)),
        )
        if not invoices:
            return False
        invoice = rng.choice(invoices)
        currency = invoice["WAERK"]
        item = rng.choice(
            self._rows(con, "SELECT * FROM VBRP WHERE VBELN = ? ORDER BY POSNR", invoice["VBELN"])
        )
        share = Decimal(rng.choice([10, 20, 50])) / 100
        credit = -(from_sap_amount(item["NETWR"], currency) * share).quantize(
            Decimal(10) ** -CURRENCY_DECIMALS.get(currency, 2), ROUND_HALF_UP
        )
        memo = f"{self._next_number(con, 'NR_BILLING', 90000000):010d}"
        self._insert(
            con,
            "VBRP",
            {
                "MANDT": CLIENT,
                "VBELN": memo,
                "POSNR": "000010",
                "VGBEL": invoice["VBELN"],
                "VGPOS": item["POSNR"],
                "AUBEL": item["AUBEL"],
                "AUPOS": item["AUPOS"],
                "MATNR": item["MATNR"],
                "FKIMG": "0.000",
                "NETWR": to_sap_amount(credit, currency),
            },
        )
        self._insert(
            con,
            "VBRK",
            {
                "MANDT": CLIENT,
                "VBELN": memo,
                "FKART": "G2",
                "FKDAT": sap_date(day),
                "KUNRG": invoice["KUNRG"],
                "NETWR": to_sap_amount(credit, currency),
                "WAERK": currency,
            },
        )
        return True

    # ------------------------------------------------------------------ DML with change logging
    def _insert(self, con: sqlite3.Connection, table: str, row: dict[str, str]) -> None:
        t = TABLES[table]
        full = {f: row.get(f, "") for f in t.fields}
        placeholders = ", ".join("?" * len(t.fields))
        con.execute(
            f"INSERT INTO {table} ({', '.join(t.fields)}) VALUES ({placeholders})",
            [full[f] for f in t.fields],
        )
        self._log(con, table, full, "I")

    def _update(
        self, con: sqlite3.Connection, table: str, key: dict[str, str], changes: dict[str, str]
    ) -> None:
        t = TABLES[table]
        con.execute(
            f"UPDATE {table} SET {', '.join(f'{c} = ?' for c in changes)} "
            f"WHERE {' AND '.join(f'{k} = ?' for k in t.keys)}",
            [*changes.values(), *(key[k] for k in t.keys)],
        )
        self._log(con, table, key, "U")

    def _delete(self, con: sqlite3.Connection, table: str, key: dict[str, str]) -> None:
        t = TABLES[table]
        con.execute(
            f"DELETE FROM {table} WHERE {' AND '.join(f'{k} = ?' for k in t.keys)}",
            [key[k] for k in t.keys],
        )
        self._log(con, table, key, "D")

    def _log(self, con: sqlite3.Connection, table: str, key: dict[str, str], op: str) -> None:
        con.execute(
            "INSERT INTO ZODP_DELTA_LOG (TABNAME, TABKEY, OP, LOGGED_AT) VALUES (?, ?, ?, ?)",
            (table, TABLES[table].tabkey(key), op, self._tick().isoformat(sep=" ")),
        )

    # ------------------------------------------------------------------ helpers
    def _tick(self) -> datetime:
        self._clock += timedelta(seconds=37)
        return self._clock

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        return con

    @staticmethod
    def _rows(con: sqlite3.Connection, sql: str, *params: str) -> list[dict[str, str]]:
        return [dict(r) for r in con.execute(sql, params).fetchall()]

    @staticmethod
    def _get_state(con: sqlite3.Connection, name: str) -> str:
        return con.execute("SELECT VALUE FROM ZSIM_STATE WHERE NAME = ?", (name,)).fetchone()[0]

    @staticmethod
    def _set_state(con: sqlite3.Connection, name: str, value: str) -> None:
        con.execute("INSERT OR REPLACE INTO ZSIM_STATE (NAME, VALUE) VALUES (?, ?)", (name, value))

    def _next_number(self, con: sqlite3.Connection, range_name: str, base: int) -> int:
        """SAP number ranges: each document type draws from its own interval."""
        row = con.execute("SELECT VALUE FROM ZSIM_STATE WHERE NAME = ?", (range_name,)).fetchone()
        current = int(row[0]) + 1 if row else base + 1
        self._set_state(con, range_name, str(current))
        return current
