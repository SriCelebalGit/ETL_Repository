# Databricks notebook source
# MAGIC %md
# MAGIC # 90 - Batch summary
# MAGIC
# MAGIC Runs with `run_if: ALL_DONE`, so it reports on a batch whether or not the layers
# MAGIC succeeded - which is exactly when a summary is most useful.
# MAGIC
# MAGIC It also fails the task when a layer task failed. Without that, a master run whose
# MAGIC bronze feeds all failed would still finish "green" because the summary task itself
# MAGIC succeeded.

# COMMAND ----------

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent / "src"))

from framework.runtime import bootstrap  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("environment", "free", "Environment")
dbutils.widgets.text("batch_id", "", "Batch id")

environment = dbutils.widgets.get("environment").strip()
batch_id = dbutils.widgets.get("batch_id").strip()

if not batch_id:
    raise ValueError("batch_id is required - it is set by the discover task")

rt = bootstrap(environment=environment, batch_id=batch_id, layer="audit")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per layer outcome

layer_summary = rt.spark.sql(
    f"""
    SELECT layer,
           COUNT(*)                                                  AS tasks,
           SUM(CASE WHEN job_status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
           SUM(CASE WHEN job_status = 'FAILED'    THEN 1 ELSE 0 END) AS failed,
           SUM(CASE WHEN job_status = 'RUNNING'   THEN 1 ELSE 0 END) AS still_running,
           SUM(COALESCE(records_read, 0))                            AS records_read,
           SUM(COALESCE(records_inserted, 0))                        AS records_inserted,
           SUM(COALESCE(records_updated, 0))                         AS records_updated,
           SUM(COALESCE(records_rejected, 0))                        AS records_rejected,
           ROUND(SUM(COALESCE(duration_seconds, 0)), 1)              AS total_task_seconds
    FROM {rt.cfg.audit_table('job_run_audit')}
    WHERE batch_id = :batch_id
    GROUP BY layer
    ORDER BY CASE layer WHEN 'control' THEN 1 WHEN 'bronze' THEN 2
                        WHEN 'silver' THEN 3 WHEN 'gold' THEN 4 ELSE 5 END
    """,
    args={"batch_id": batch_id},
)
display(layer_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failures in this batch

failures = rt.spark.sql(
    f"""
    SELECT layer, catalog_name, schema_name, table_name, job_status,
           error_message, task_start_timestamp, duration_seconds
    FROM {rt.cfg.audit_table('job_run_audit')}
    WHERE batch_id = :batch_id
      AND job_status IN ('FAILED', 'RUNNING')
    ORDER BY layer, table_name
    """,
    args={"batch_id": batch_id},
)
display(failures)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality in this batch

display(
    rt.spark.sql(
        f"""
        SELECT source_schema_name, table_name, dq_check_outcome,
               rules_evaluated, rules_failed, src_rec_count,
               quarantine_count, warning_count, quarantine_pct
        FROM {rt.cfg.audit_table('dq_run_audit')}
        WHERE batch_id = :batch_id
        ORDER BY quarantine_pct DESC, table_name
        """,
        args={"batch_id": batch_id},
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worst performing DQ rules

display(
    rt.spark.sql(
        f"""
        SELECT table_name, column_name, rule_id, severity,
               rows_evaluated, rows_failed, pass_pct
        FROM {rt.cfg.audit_table('dq_result_detail')}
        WHERE batch_id = :batch_id
          AND rows_failed > 0
        ORDER BY rows_failed DESC
        LIMIT 50
        """,
        args={"batch_id": batch_id},
    )
)

# COMMAND ----------

failed_count = failures.filter("job_status = 'FAILED'").count()
stuck_count = failures.filter("job_status = 'RUNNING'").count()

rt.log.info("batch summary complete", failed_tasks=failed_count, stuck_tasks=stuck_count)

if failed_count or stuck_count:
    raise RuntimeError(
        f"Batch {batch_id} finished with {failed_count} failed and {stuck_count} unfinished task(s). "
        f"See {rt.cfg.audit_table('job_run_audit')} for the detail."
    )

dbutils.notebook.exit(f"OK batch_id={batch_id}")
