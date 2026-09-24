# 003. DuckDB as the dbt engine, with a warehouse swap kept to a config change

**Status:** Accepted

## Context

The project has to run anywhere with one command, including in CI on every push, with no cloud account and no cost. It should still look like something that could move to a production warehouse without a rewrite.

## Decision

dbt runs on DuckDB through dbt-duckdb, and reads the bronze Iceberg tables through the adapter's iceberg plugin (PyIceberg). Models use standard SQL plus a few DuckDB conveniences (`qualify`, `exclude`, `filter`) that Snowflake, Databricks and Trino also support. SAP-specific logic lives in macros, so an adapter-specific variant can be added in one place.

## Consequences

- The full pipeline, including 44 dbt tests, runs in about five seconds locally and in CI.
- Moving to Athena, Trino, Snowflake or Databricks means changing `profiles.yml` and the source definitions, not the models.
- dbt-duckdb reloads a plugin-backed source each time it is referenced, so every bronze source is read by exactly one staging model. That is also good dbt practice.
