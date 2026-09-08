# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Task discovery
# MAGIC
# MAGIC Reads the control tables and publishes, as task values, the list of tables each layer
# MAGIC must load. The layer jobs consume these with a `for_each` task, so adding a feed is a
# MAGIC YAML commit - no job definition changes, no redeploy.
# MAGIC
# MAGIC Task values published
# MAGIC - `bronze_tables`  `[{catalog, schema, table, load_type}, ...]`
# MAGIC - `silver_tables`  `[{catalog, schema, table, load_type}, ...]`
# MAGIC - `gold_dimensions` / `gold_facts` / `gold_aggregates` - split so facts can be made
# MAGIC   to depend on dimensions with three ordered `for_each` tasks
# MAGIC - `batch_id`       the master batch id every layer task is given
# MAGIC
# MAGIC Streaming bronze feeds (`load_type = stream`) are excluded from the batch list by
# MAGIC default: a continuous query never terminates and would hold the batch job open.
# MAGIC Deploy those as their own continuous job.

# COMMAND ----------

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent / "src"))

from framework.runtime import bootstrap  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("environment", "free", "Environment")
dbutils.widgets.text("source_system", "", "Bronze source system filter (blank = all)")
dbutils.widgets.dropdown("include_streaming", "false", ["true", "false"], "Include streaming feeds")

environment = dbutils.widgets.get("environment").strip()
source_system = dbutils.widgets.get("source_system").strip() or None
include_streaming = dbutils.widgets.get("include_streaming") == "true"

rt = bootstrap(environment=environment, layer="control")

# COMMAND ----------

bronze_configs = rt.repo.list_bronze_configs(source_system=source_system)
if not include_streaming:
    bronze_configs = [b for b in bronze_configs if b.load_type == "batch"]

bronze_tables = [
    {"catalog": b.catalog, "schema": b.schema, "table": b.table, "load_type": b.load_type}
    for b in bronze_configs
]

silver_tables = [
    {
        "catalog": s.target_catalog,
        "schema": s.target_schema,
        "table": s.target_table,
        "load_type": s.load_type,
    }
    for s in rt.repo.list_silver_configs()
]

gold_configs = rt.repo.list_gold_configs()


def gold_entries(object_types):
    return [
        {
            "catalog": g.target_catalog,
            "schema": g.target_schema,
            "table": g.table,
            "load_type": g.load_type,
        }
        for g in gold_configs
        if (g.object_type or "").lower() in object_types
    ]


gold_dimensions = gold_entries({"dimension", "bridge"})
gold_facts = gold_entries({"fact"})
gold_aggregates = gold_entries({"aggregate"})
# An object with no object_type still has to run; grouping it with the facts is the
# safe default, since anything unclassified is more likely to read a dimension than
# to be one.
gold_facts += [
    {
        "catalog": g.target_catalog,
        "schema": g.target_schema,
        "table": g.table,
        "load_type": g.load_type,
    }
    for g in gold_configs
    if (g.object_type or "").lower() not in {"dimension", "bridge", "fact", "aggregate"}
]

rt.log.info(
    "task discovery complete",
    bronze_count=len(bronze_tables),
    silver_count=len(silver_tables),
    gold_dimension_count=len(gold_dimensions),
    gold_fact_count=len(gold_facts),
    gold_aggregate_count=len(gold_aggregates),
)

# COMMAND ----------

# A for_each task reads its inputs from a task value as a JSON array string.
dbutils.jobs.taskValues.set("bronze_tables", json.dumps(bronze_tables))
dbutils.jobs.taskValues.set("silver_tables", json.dumps(silver_tables))
dbutils.jobs.taskValues.set("gold_dimensions", json.dumps(gold_dimensions))
dbutils.jobs.taskValues.set("gold_facts", json.dumps(gold_facts))
dbutils.jobs.taskValues.set("gold_aggregates", json.dumps(gold_aggregates))
dbutils.jobs.taskValues.set("batch_id", rt.batch_id)

# COMMAND ----------

display(
    rt.spark.createDataFrame(
        [("bronze", len(bronze_tables)), ("silver", len(silver_tables)),
         ("gold_dimensions", len(gold_dimensions)), ("gold_facts", len(gold_facts)),
         ("gold_aggregates", len(gold_aggregates))],
        "layer string, table_count int",
    )
)

# COMMAND ----------

# An empty layer is a configuration problem worth failing on: a job that "succeeds"
# having loaded nothing is the failure mode that goes unnoticed for weeks.
if not bronze_tables and not silver_tables and not gold_dimensions and not gold_facts:
    raise ValueError(
        "No enabled control table rows found. Run notebooks/01_load_control_tables.py, "
        "and check is_enabled on the control rows."
    )

dbutils.notebook.exit(f"OK batch_id={rt.batch_id}")
