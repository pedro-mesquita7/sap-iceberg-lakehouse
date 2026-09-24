"""Command-line entry point: `sap-lakehouse <command>`."""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from datetime import UTC, date, datetime, timedelta

from sap_lakehouse.config import DBT_PROJECT_DIR, erp_db_path, get_catalog, lakehouse_home
from sap_lakehouse.erp.schema import TABLES
from sap_lakehouse.erp.simulator import DaySummary, ErpSimulator
from sap_lakehouse.extract.odp import ExtractResult, OdpExtractor, bronze_identifier


def _print_days(summaries: list[DaySummary]) -> None:
    for s in summaries:
        activity = ", ".join(f"{k.replace('_', ' ')}: {v}" for k, v in sorted(s.counts.items()))
        print(f"  {s.day:%a %Y-%m-%d}  {activity or 'no activity'}")


def _print_extract(results: list[ExtractResult]) -> None:
    print(f"  {'table':<6} {'load':<6} {'ins':>5} {'upd':>5} {'del':>5} {'watermark':>10}")
    for r in results:
        print(
            f"  {r.table:<6} {r.load_type:<6} {r.inserts:>5} {r.updates:>5} {r.deletes:>5} "
            f"{r.watermark if r.watermark is not None else '-':>10}"
        )


def _simulator(args: argparse.Namespace) -> ErpSimulator:
    return ErpSimulator(erp_db_path(), seed=args.seed)


def cmd_erp_init(args: argparse.Namespace) -> None:
    start = args.start or date.today() - timedelta(days=args.history_days)
    print(f"Creating SAP system with {args.history_days} days of history from {start}")
    _print_days(_simulator(args).initialize(start, args.history_days))


def cmd_erp_advance(args: argparse.Namespace) -> None:
    _print_days(_simulator(args).advance(args.days))


def cmd_extract(args: argparse.Namespace) -> None:
    extractor = OdpExtractor(erp_db_path(), get_catalog())
    tables = [args.table.upper()] if args.table else list(TABLES)
    _print_extract([extractor.extract_table(t) for t in tables])


def run_dbt(*dbt_args: str) -> None:
    from dbt.cli.main import dbtRunner

    os.environ["SAP_LAKEHOUSE_HOME"] = lakehouse_home().as_posix()
    result = dbtRunner().invoke(
        [*dbt_args, "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)]
    )
    if not result.success:
        sys.exit(1)


def cmd_transform(args: argparse.Namespace) -> None:
    run_dbt("build")


def cmd_docs(args: argparse.Namespace) -> None:
    run_dbt("docs", "generate", "--static")
    print(f"Static docs site: {DBT_PROJECT_DIR / 'target' / 'static_index.html'}")


def cmd_snapshots(args: argparse.Namespace) -> None:
    """Show the Iceberg snapshot history of a bronze table - every one is a time-travel point."""
    table = get_catalog().load_table(bronze_identifier(TABLES[args.table.upper()]))
    print(
        f"  {'snapshot_id':<20} {'committed (UTC)':<20} {'load':<6} {'rows':>6} {'watermark':>10}"
    )
    for entry in table.history():
        props = table.snapshot_by_id(entry.snapshot_id).summary.additional_properties
        committed = datetime.fromtimestamp(entry.timestamp_ms / 1000, UTC)
        print(
            f"  {entry.snapshot_id:<20} {committed:%Y-%m-%d %H:%M:%S}  "
            f"{props.get('sap.odp.load_type', ''):<6} {props.get('added-records', ''):>6} "
            f"{props.get('sap.odp.watermark', ''):>10}"
        )


def cmd_demo(args: argparse.Namespace) -> None:
    """Build the whole lakehouse from scratch: history, initial load, daily deltas, dbt build."""
    cmd_reset(args)
    start = args.start or date.today() - timedelta(days=args.history_days + args.days)
    simulator = _simulator(args)
    extractor = OdpExtractor(erp_db_path(), get_catalog())

    print(f"\n[1/3] SAP system with {args.history_days} days of history from {start}")
    simulator.initialize(start, args.history_days)
    print("\n[2/3] Initial load into Iceberg, then one delta extraction per business day")
    _print_extract(extractor.extract_all())
    for _ in range(args.days):
        summaries = simulator.advance(1)
        print()
        _print_days(summaries)
        _print_extract([r for r in extractor.extract_all() if r.load_type != "NONE"])
    print("\n[3/3] dbt build: silver + gold models and all data tests")
    run_dbt("build")


def cmd_reset(args: argparse.Namespace) -> None:
    home = lakehouse_home()
    shutil.rmtree(home, ignore_errors=True)
    home.mkdir(parents=True, exist_ok=True)
    print(f"Reset lakehouse state in {home}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sap-lakehouse", description=__doc__)
    parser.add_argument("--seed", type=int, default=42, help="simulator random seed")
    sub = parser.add_subparsers(dest="command", required=True)

    erp = sub.add_parser("erp", help="simulated SAP system").add_subparsers(
        dest="erp_cmd", required=True
    )
    p = erp.add_parser("init", help="create the SAP system with history")
    p.add_argument("--history-days", type=int, default=30)
    p.add_argument("--start", type=date.fromisoformat, help="first simulated day (YYYY-MM-DD)")
    p.set_defaults(func=cmd_erp_init)
    p = erp.add_parser("advance", help="simulate the next business day(s)")
    p.add_argument("--days", type=int, default=1)
    p.set_defaults(func=cmd_erp_advance)

    p = sub.add_parser("extract", help="CDC extraction into Iceberg bronze")
    p.add_argument("--table", choices=[t.lower() for t in TABLES] + list(TABLES))
    p.set_defaults(func=cmd_extract)

    sub.add_parser("transform", help="dbt build (silver + gold + tests)").set_defaults(
        func=cmd_transform
    )
    sub.add_parser("docs", help="generate the static dbt docs site").set_defaults(func=cmd_docs)

    p = sub.add_parser("snapshots", help="Iceberg snapshot history of a bronze table")
    p.add_argument("table", choices=[t.lower() for t in TABLES] + list(TABLES))
    p.set_defaults(func=cmd_snapshots)

    p = sub.add_parser("demo", help="build everything from scratch")
    p.add_argument("--history-days", type=int, default=30)
    p.add_argument("--days", type=int, default=5, help="daily delta runs after the initial load")
    p.add_argument("--start", type=date.fromisoformat)
    p.set_defaults(func=cmd_demo)

    sub.add_parser("reset", help="delete all local lakehouse state").set_defaults(func=cmd_reset)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
