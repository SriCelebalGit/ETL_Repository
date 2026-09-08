# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Generic bronze loader (Auto Loader)
# MAGIC
# MAGIC The generic execution notebook of the design's bronze layer. One workflow task per
# MAGIC bronze table calls this notebook with the target's identity; everything else - source
# MAGIC path, file format, reader options, trigger, write mode, physical layout - comes from
# MAGIC `bronze_control_table`.
# MAGIC
# MAGIC Widgets
# MAGIC - `environment`        which `conf/framework.<env>.yml` to use
# MAGIC - `catalog_name`       logical (`bronze`) or physical target catalog
# MAGIC - `schema_name`        bronze schema
# MAGIC - `table_name`         bronze table
# MAGIC - `batch_id`           master batch id, passed down from the parent workflow
# MAGIC - `await_timeout_seconds` stop a streaming feed after N seconds; blank means run
# MAGIC                        until the backlog is drained (batch) or forever (stream)

# COMMAND ----------

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent / "src"))

from framework.ingestion import AutoLoaderIngestor  # noqa: E402
from framework.runtime import bootstrap  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("environment", "free", "Environment")
dbutils.widgets.text("catalog_name", "bronze", "Target catalog (logical or physical)")
dbutils.widgets.text("schema_name", "", "Bronze schema")
dbutils.widgets.text("table_name", "", "Bronze table")
dbutils.widgets.text("batch_id", "", "Batch id (from the parent workflow)")
dbutils.widgets.text("await_timeout_seconds", "", "Await timeout (seconds, blank = unbounded)")

environment = dbutils.widgets.get("environment").strip()
catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
table_name = dbutils.widgets.get("table_name").strip()
batch_id = dbutils.widgets.get("batch_id").strip() or None
timeout_raw = dbutils.widgets.get("await_timeout_seconds").strip()
await_timeout_seconds = int(timeout_raw) if timeout_raw else None

if not schema_name or not table_name:
    raise ValueError("schema_name and table_name are required - the task is not parameterised correctly")

# COMMAND ----------

rt = bootstrap(environment=environment, batch_id=batch_id, layer="bronze")

# The control row is looked up by the catalog token as written in the metadata, so both
# `bronze` and the physical catalog name work as the widget value.
bronze = rt.repo.get_bronze_config(catalog_name, schema_name, table_name)

rt.log.info(
    "bronze configuration resolved",
    target=bronze.full_name,
    source_system=bronze.source_system,
    file_location=bronze.file_location,
    source_file_type=bronze.source_file_type,
    load_type=bronze.load_type,
    write_mode=bronze.write_mode,
    control_row_id=bronze.control_row_id,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest
# MAGIC
# MAGIC The audit context manager records a RUNNING row, then SUCCEEDED with the metrics or
# MAGIC FAILED with the stack trace - so a killed task still leaves evidence behind.

ingestor = AutoLoaderIngestor(rt.spark, rt.cfg, rt.audit, rt.log)

with rt.audit.task(
    layer="bronze",
    catalog_name=bronze.catalog,
    schema_name=bronze.schema,
    table_name=bronze.table,
    source_object=bronze.file_location,
    control_row_id=bronze.control_row_id,
) as metrics:
    result = ingestor.ingest(bronze, await_timeout_seconds=await_timeout_seconds)
    for field, value in vars(result).items():
        setattr(metrics, field, value)

# COMMAND ----------

dbutils.notebook.exit(
    f"OK table={bronze.full_name} read={metrics.records_read} inserted={metrics.records_inserted} "
    f"files={metrics.files_processed} batch_id={rt.batch_id}"
)
