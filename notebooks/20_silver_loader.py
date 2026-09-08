# Databricks notebook source
# MAGIC %md
# MAGIC # 20 - Generic silver loader (DQ + SCD)
# MAGIC
# MAGIC The generic DQ/curation notebook of the design's silver layer. One workflow task per
# MAGIC silver table calls it; the control tables supply the source, the load pattern, the
# MAGIC business keys and the DQ rule assignments.
# MAGIC
# MAGIC Severity semantics
# MAGIC - `drop`    - failing rows go to the quarantine table and are excluded from silver
# MAGIC - `warning` - failing rows go to quarantine AND are still loaded
# MAGIC - `fail`    - any failing row aborts the task and nothing is loaded

# COMMAND ----------

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent / "src"))

from framework.runtime import bootstrap  # noqa: E402
from framework.transform import SilverLoader  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("environment", "free", "Environment")
dbutils.widgets.text("catalog_name", "silver", "Target catalog (logical or physical)")
dbutils.widgets.text("schema_name", "", "Silver schema")
dbutils.widgets.text("table_name", "", "Silver table")
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

rt = bootstrap(environment=environment, batch_id=batch_id, layer="silver")
silver = rt.repo.get_silver_config(catalog_name, schema_name, table_name)

rt.log.info(
    "silver configuration resolved",
    target=silver.target_full_name,
    source=silver.source_full_name,
    load_type=silver.load_type,
    read_mode=silver.read_mode,
    business_keys=silver.business_keys,
    dq_enabled=silver.dq_enabled,
    control_row_id=silver.control_row_id,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Curate
# MAGIC
# MAGIC DQ results land in `dq_run_audit` and `dq_result_detail` regardless of the outcome, so
# MAGIC a quarantine spike is visible even when the task itself succeeds.

loader = SilverLoader(rt.spark, rt.cfg, rt.repo, rt.audit, rt.log)

with rt.audit.task(
    layer="silver",
    catalog_name=silver.target_catalog,
    schema_name=silver.target_schema,
    table_name=silver.target_table,
    source_object=silver.source_full_name,
    control_row_id=silver.control_row_id,
) as metrics:
    result = loader.load(silver, await_timeout_seconds=await_timeout_seconds)
    for field, value in vars(result).items():
        setattr(metrics, field, value)

# COMMAND ----------

# MAGIC %md
# MAGIC ## This batch's DQ outcome

display(
    rt.spark.sql(
        f"""
        SELECT column_name, rule_id, severity, rows_evaluated, rows_failed, pass_pct, rule_status
        FROM {rt.cfg.audit_table('dq_result_detail')}
        WHERE batch_id = :batch_id
          AND lower(table_name) = lower(:table_name)
        ORDER BY rows_failed DESC, column_name
        """,
        args={"batch_id": rt.batch_id, "table_name": silver.source_table},
    )
)

# COMMAND ----------

dbutils.notebook.exit(
    f"OK table={silver.target_full_name} read={metrics.records_read} inserted={metrics.records_inserted} "
    f"updated={metrics.records_updated} quarantined={metrics.records_rejected} batch_id={rt.batch_id}"
)
