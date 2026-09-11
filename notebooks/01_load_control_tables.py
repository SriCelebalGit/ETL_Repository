# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 - Control table load pipeline
# MAGIC
# MAGIC Ingests the YAML under `conf/metadata/` into the control tables as SCD Type-2.
# MAGIC This is the "control table load pipeline" of the design's user journey, and the CI/CD
# MAGIC entry point: an application developer's only deployment action is committing YAML.
# MAGIC
# MAGIC Widgets
# MAGIC - `environment`     which `conf/framework.<env>.yml` to use
# MAGIC - `tables`          comma-separated control tables to load, or `all`
# MAGIC - `prune_missing`   end-date control rows whose YAML entry was deleted
# MAGIC - `dry_run`         validate and report drift without writing
# MAGIC
# MAGIC Run with `dry_run=true` on a pull request to fail the build on invalid metadata, then
# MAGIC with `dry_run=false` on merge.

# COMMAND ----------

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent / "src"))

from framework.control import MetadataLoader, TABLE_SPECS  # noqa: E402
from framework.runtime import bootstrap  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("environment", "free", "Environment")
dbutils.widgets.text("tables", "all", "Control tables (comma separated, or all)")
dbutils.widgets.dropdown("prune_missing", "true", ["true", "false"], "Prune deleted entries")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry run")

environment = dbutils.widgets.get("environment").strip()
requested = dbutils.widgets.get("tables").strip()
prune_missing = dbutils.widgets.get("prune_missing") == "true"
dry_run = dbutils.widgets.get("dry_run") == "true"

rt = bootstrap(environment=environment, layer="control")
loader = MetadataLoader(rt.spark, rt.cfg, logger=rt.log)

# COMMAND ----------

# MAGIC %md
# MAGIC Load
# MAGIC
# MAGIC The entire load process is wrapped inside an audit context so that both metadata validation failures and data pipeline execution metrics are captured in job_run_audit. This makes operational monitoring simpler because framework-level issues and data loading activities are audited in a single place.

# COMMAND ----------

with rt.audit.task(
    layer="control",
    catalog_name=rt.cfg.framework_catalog,
    schema_name=rt.cfg.control_schema,
) as metrics:

    if requested.lower() == "all":
        results = loader.load_all(
            prune_missing=prune_missing,
            dry_run=dry_run,
        )
    else:
        names = [
            n.strip()
            for n in requested.split(",")
            if n.strip()
        ]

        unknown = [
            n
            for n in names
            if n not in TABLE_SPECS
        ]

        if unknown:
            raise ValueError(
                f"Unknown control table(s) {unknown}. "
                f"Valid: {sorted(TABLE_SPECS)}"
            )

        results = {
            name: loader.load_table(
                TABLE_SPECS[name],
                prune_missing=prune_missing,
                dry_run=dry_run,
            )
            for name in names
        }

    metrics.records_read = sum(
        r.get("parsed", 0)
        for r in results.values()
    )

    metrics.records_inserted = sum(
        r.get("inserted", 0)
        for r in results.values()
    )

    metrics.records_updated = sum(
        r.get("closed", 0)
        for r in results.values()
    )

    metrics.records_deleted = sum(
        r.get("pruned", 0)
        for r in results.values()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Result
# MAGIC
# MAGIC The load results are converted into a Spark DataFrame and displayed as a summary report. This provides a consolidated view of records processed for each control table.
# MAGIC
# MAGIC What the Summary Shows
# MAGIC control_table: Name of the control table being processed.
# MAGIC yaml_records: Number of records parsed from the metadata configuration.
# MAGIC versions_inserted: New records or SCD2 versions inserted.
# MAGIC versions_closed: Existing versions closed as part of SCD2 processing.
# MAGIC rows_pruned: Records removed due to pruning logic.
# MAGIC would_change: Number of rows that would be affected in dry_run mode. Displays NULL during an actual load.

# COMMAND ----------

summary = rt.spark.createDataFrame(
    [
        (
            table,
            int(r.get("parsed", 0)),
            int(r.get("inserted", 0)),
            int(r.get("closed", 0)),
            int(r.get("pruned", 0)),
            int(r.get("would_change", 0)) if dry_run else None,
        )
        for table, r in results.items()
    ],
    (
        "control_table string, "
        "yaml_records int, "
        "versions_inserted int, "
        "versions_closed int, "
        "rows_pruned int, "
        "would_change int"
    ),
)

display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# A dry run that finds drift should fail the CI check, so the pull request shows that the
# metadata differs from what is deployed.
if dry_run:
    drift = sum(r.get("would_change", 0) for r in results.values())
    dbutils.notebook.exit(f"DRY_RUN drift_rows={drift}")

dbutils.notebook.exit(
    f"OK inserted={sum(r.get('inserted', 0) for r in results.values())} "
    f"closed={sum(r.get('closed', 0) for r in results.values())} "
    f"pruned={sum(r.get('pruned', 0) for r in results.values())}"
)