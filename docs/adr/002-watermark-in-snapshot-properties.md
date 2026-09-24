# 002. The extraction watermark lives in the Iceberg snapshot summary

**Status:** Accepted

## Context

A CDC extractor must remember how far it has read. Storing that watermark in a separate state table or file creates a two-phase problem. If data is committed but the watermark is not, the next run reloads the same window. If the watermark is committed but the data is not, changes are lost silently.

## Decision

The watermark is written as a snapshot summary property (`sap.odp.watermark`) in the same Iceberg commit as the appended events. The current watermark is read from the table's current snapshot. No snapshot means an initial load.

## Consequences

- Data and progress are committed atomically. After a crash the window is simply read again, and silver's latest-event-per-key logic absorbs any overlap.
- Resetting a table's extraction is a normal Iceberg operation: roll back to a snapshot and the watermark rolls back with it.
- The load type and batch id of every run travel with the snapshot too, which makes the snapshot history an audit log of the extraction.
