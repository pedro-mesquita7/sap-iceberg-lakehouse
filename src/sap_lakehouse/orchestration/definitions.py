"""Dagster code location: SAP ERP -> Iceberg bronze -> dbt silver/gold as one asset graph.

Run `dagster dev` from the repo root and open http://localhost:3000 to see the lineage.
dbt tests show up as asset checks on the models they test.
"""

import os
import sys
from collections.abc import Iterator, Mapping
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from sap_lakehouse.config import DBT_PROJECT_DIR, erp_db_path, get_catalog, lakehouse_home
from sap_lakehouse.erp.schema import TABLES
from sap_lakehouse.erp.simulator import ErpSimulator
from sap_lakehouse.extract.odp import OdpExtractor

# dbt's profiles.yml reads the lakehouse location from the environment
os.environ.setdefault("SAP_LAKEHOUSE_HOME", lakehouse_home().as_posix())
# Make the dbt CLI installed next to this interpreter findable even when the venv is not activated
os.environ["PATH"] = os.pathsep.join([str(Path(sys.executable).parent), os.environ.get("PATH", "")])

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()

ERP_BUSINESS_DAY = dg.AssetKey(["sap_erp", "business_day"])


@dg.asset(
    key=ERP_BUSINESS_DAY,
    group_name="sap_erp",
    kinds={"sap", "sqlite"},
    description=(
        "Simulates one business day in the SAP system "
        "(creates it with 30 days of history on first run)."
    ),
)
def erp_business_day() -> dg.MaterializeResult:
    simulator = ErpSimulator(erp_db_path())
    if erp_db_path().exists():
        summaries = simulator.advance(1)
    else:
        summaries = simulator.initialize(date.today() - timedelta(days=30), history_days=30)
    day = summaries[-1]
    return dg.MaterializeResult(metadata={"simulated_day": str(day.day), **day.counts})


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            key=["bronze", name.lower()],
            group_name="bronze",
            deps=[ERP_BUSINESS_DAY],
            kinds={"iceberg", "python"},
            description=f"SAP {name}: {table.description}. Append-only CDC events in Iceberg.",
        )
        for name, table in TABLES.items()
    ],
    can_subset=True,
)
def sap_bronze(context: dg.AssetExecutionContext) -> Iterator[dg.MaterializeResult]:
    """ODP-style delta extraction; the watermark commits atomically with each snapshot."""
    extractor = OdpExtractor(erp_db_path(), get_catalog())
    for name in TABLES:  # headers before items, master data first
        key = dg.AssetKey(["bronze", name.lower()])
        if key not in context.selected_asset_keys:
            continue
        result = extractor.extract_table(name)
        yield dg.MaterializeResult(
            asset_key=key,
            metadata={
                "load_type": result.load_type,
                "inserts": result.inserts,
                "updates": result.updates,
                "deletes": result.deletes,
                "watermark": result.watermark or 0,
                "iceberg_snapshot_id": str(result.snapshot_id or "unchanged"),
            },
        )


class LakehouseTranslator(DagsterDbtTranslator):
    """Map dbt sources onto the bronze assets above and group models by medallion layer."""

    def get_asset_spec(
        self, manifest: Mapping[str, Any], unique_id: str, project: DbtProject | None
    ) -> dg.AssetSpec:
        spec = super().get_asset_spec(manifest, unique_id, project)
        props = self.get_resource_props(manifest, unique_id)
        if props["resource_type"] == "source":
            return spec.replace_attributes(key=dg.AssetKey(["bronze", props["name"]]))
        if props["resource_type"] == "seed":
            return spec.replace_attributes(group_name="reference")
        layer = "silver" if "staging" in props["fqn"] else "gold"
        return spec.replace_attributes(group_name=layer)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    dagster_dbt_translator=LakehouseTranslator(),
)
def sap_dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


order_to_cash_daily = dg.define_asset_job(
    "order_to_cash_daily",
    selection=dg.AssetSelection.all(),
    description="Simulate a business day, land its deltas in Iceberg, rebuild silver and gold.",
)

defs = dg.Definitions(
    assets=[erp_business_day, sap_bronze, sap_dbt_models],
    jobs=[order_to_cash_daily],
    schedules=[
        dg.ScheduleDefinition(
            job=order_to_cash_daily, cron_schedule="0 6 * * *", execution_timezone="Europe/Lisbon"
        )
    ],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
