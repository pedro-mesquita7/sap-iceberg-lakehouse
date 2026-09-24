# 001. Bronze is an append-only CDC event log in Iceberg

**Status:** Accepted

## Context

SAP extractions usually land in one of two shapes. A *mirror* (MERGE every change into a copy of the source table) is simple to query but throws away history and makes re-runs dangerous. An *event log* (append every change) keeps everything, but every consumer then has to resolve the latest state itself.

## Decision

Bronze tables are append-only event logs: one row per extracted change, with `_op`, `_seq`, `_changed_at` and `_extracted_at` metadata. They are stored in Apache Iceberg, partitioned by extraction day. Current state is resolved once, in silver, by a single dbt macro (`sap_current_state`).

## Consequences

- Re-running an extraction can only add duplicate events, which silver already deduplicates by `_seq`. There is no MERGE to get wrong.
- Full change history is available, so `dim_customer_history` (SCD Type 2) is built from real change timestamps instead of snapshots that only see the state at run time.
- Every load is an Iceberg snapshot, so any past bronze state can be queried with time travel (`sap-lakehouse snapshots <table>`).
- Bronze grows with every change. In production you would add snapshot expiry and compaction on a schedule.
