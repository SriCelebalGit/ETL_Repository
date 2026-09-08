# Databricks notebook source
# MAGIC %md
# MAGIC # 30 - Generic gold loader
# MAGIC
# MAGIC The generic transformation notebook of the design's gold layer. One workflow task per
# MAGIC dimension or fact calls it; `gold_control_table` says which transformation to run
# MAGIC (module, SQL file or notebook) and how the result is loaded.
# MAGIC
# MAGIC The application developer writes only the transformation - the framework owns the read
# MAGIC of the control row, the write pattern, the physical layout and the audit.

# COMMAND ----------

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent / "src"))

from framework.gold import GoldRunner  # noqa: E402
from framework.runtime import bootstrap  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("environment", "free", "Environment")
dbutils.widgets.text("catalog_name", "gold", "Target catalog (logical or physical)")
dbutils.widgets.text("schema_name", "", "Gold schema")
dbutils.widgets.text("table_name", "", "Gold table")
dbutils.widgets.text("batch_id", "", "Batch id (from the parent workflow)")

environment = dbutils.widgets.get("environment").strip()
catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
table_name = dbutils.widgets.get("table_name").strip()
batch_id = dbutils.widgets.get("batch_id").strip() or None

if not schema_name or not table_name:
    raise ValueError("schema_name and table_name are required - the task is not parameterised correctly")

# COMMAND ----------

rt = bootstrap(environment=environment, batch_id=batch_id, layer="gold")
gold = rt.repo.get_gold_config(catalog_name, schema_name, table_name)

rt.log.info(
    "gold configuration resolved",
    target=gold.full_name,
    object_type=gold.object_type,
    transformation_type=gold.transformation_type,
    load_type=gold.load_type,
    depends_on=gold.depends_on,
    control_row_id=gold.control_row_id,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform and load

runner = GoldRunner(rt.spark, rt.cfg, rt.audit, rt.log)

with rt.audit.task(
    layer="gold",
    catalog_name=gold.target_catalog,
    schema_name=gold.target_schema,
    table_name=gold.table,
    source_object=",".join(gold.depends_on) or None,
    control_row_id=gold.control_row_id,
) as metrics:
    result = runner.run(gold)
    for field, value in vars(result).items():
        setattr(metrics, field, value)

# COMMAND ----------

dbutils.notebook.exit(
    f"OK table={gold.full_name} read={metrics.records_read} inserted={metrics.records_inserted} "
    f"updated={metrics.records_updated} rows={metrics.target_row_count} batch_id={rt.batch_id}"
)
