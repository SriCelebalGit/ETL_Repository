# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 - Auto Loader smoke test (Free Edition)
# MAGIC
# MAGIC **Run this before the pipeline.** It proves, in about a minute and with no framework
# MAGIC code involved, that the two things Free Edition might not allow actually work:
# MAGIC
# MAGIC 1. Auto Loader can **read** files from a UC Volume
# MAGIC 2. Auto Loader can **write its schema store and streaming checkpoint** to a UC
# MAGIC    Volume — the load-bearing assumption of this whole build, since Free Edition has
# MAGIC    no storage credential for `abfss://`
# MAGIC
# MAGIC It also proves the incremental contract: a second run over the same directory reads
# MAGIC **zero** rows, and only picks up a newly arrived file.
# MAGIC
# MAGIC Everything it creates lives under a `_smoke_test` prefix and is dropped at the end.
# MAGIC If this notebook fails, stop — the framework will fail the same way but with more
# MAGIC moving parts in the traceback.

# COMMAND ----------

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent / "src"))

from framework.config import FrameworkConfig  # noqa: E402

dbutils.widgets.text("environment", "free", "Environment")
environment = dbutils.widgets.get("environment").strip()

cfg = FrameworkConfig.load(environment=environment)
free = cfg.free_edition

VOLUMES = f"/Volumes/{cfg.framework_catalog}/{free.get('volumes_schema', 'etl_volumes')}"
SRC = f"{VOLUMES}/{free.get('landing_volume', 'landing')}/_smoke_test"
CHK = f"{VOLUMES}/{free.get('checkpoint_volume', 'checkpoints')}/_smoke_test"
TARGET = f"{cfg.framework_catalog}.{cfg.control_schema}._smoke_test_autoloader"

print(f"source      {SRC}")
print(f"checkpoint  {CHK}")
print(f"target      {TARGET}")

# COMMAND ----------

# Clean slate, so the test is repeatable.
for path in (SRC, CHK):
    try:
        dbutils.fs.rm(path, recurse=True)
    except Exception:
        pass
spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
dbutils.fs.mkdirs(SRC)

with open(f"{SRC}/part_001.csv", "w", encoding="utf-8", newline="\n") as handle:
    handle.write("id,name,amount\n1,alpha,10.5\n2,beta,20.25\n3,gamma,30.0\n")

print("wrote part_001.csv (3 rows)")
display(dbutils.fs.ls(SRC))

# COMMAND ----------

# MAGIC %md
# MAGIC Test 1 — read from a Volume, checkpoint to a Volume

# COMMAND ----------

def run_autoloader(label: str) -> int:
    """One availableNow pass. Returns the number of rows this pass ingested."""

    def target_count() -> int:
        return spark.table(TARGET).count() if spark.catalog.tableExists(TARGET) else 0

    before = target_count()

    def handle(batch_df, batch_id):
        # Runs server-side under Spark Connect - no side effects reach this notebook.
        (
            batch_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(TARGET)
        )

    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{CHK}/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(SRC)
        .writeStream.option("checkpointLocation", f"{CHK}/_checkpoint")
        .queryName(f"smoke_{label}")
        .foreachBatch(handle)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    if query.exception() is not None:
        raise RuntimeError(f"[{label}] stream terminated with an error: {query.exception()}")

    return target_count() - before



# COMMAND ----------

first = run_autoloader("first")

print(f"PASS 1 ingested {first} row(s)")

assert first == 3, f"expected 3 rows on the first pass, got {first}"

# COMMAND ----------

# MAGIC %md
# MAGIC Test 2 — the checkpoint is real
# MAGIC A second pass over an unchanged directory must ingest nothing. If this returns 3 again, the checkpoint is not persisting to the Volume and every scheduled run would duplicate its whole source.

# COMMAND ----------

second = run_autoloader("second") 

print(f"PASS 2 ingested {second} row(s)") 

assert second == 0, ( f"expected 0 rows on an unchanged directory, got {second}. " f"The checkpoint at {CHK}/_checkpoint is not being persisted." )

# COMMAND ----------

# MAGIC %md
# MAGIC Test 3 — a new file is picked up incrementally

# COMMAND ----------

with open(f"{SRC}/part_002.csv", "w", encoding="utf-8", newline="\n") as handle:
    handle.write(
        "id,name,amount\n"
        "4,delta,40.75\n"
        "5,epsilon,50.0\n"
    )

third = run_autoloader("third")
print(f"PASS 3 ingested {third} row(s)")
assert third == 2, f"expected 2 new rows, got {third}"

total = spark.table(TARGET).count()
print(f"target now holds {total} row(s)")
assert total == 5, f"expected 5 rows in total, got {total}"

# COMMAND ----------

# MAGIC %md
# MAGIC Test 4 — schema store contents
# MAGIC Auto Loader keeps the inferred schema here. Its presence is what makes schema evolution work across runs.

# COMMAND ----------

display(dbutils.fs.ls(f"{CHK}/_schema"))

# COMMAND ----------

# MAGIC %md
# MAGIC Clean up

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TARGET}") 

for path in (SRC, CHK): dbutils.fs.rm(path, recurse=True)

print("cleaned up")

# COMMAND ----------

# MAGIC %md
# MAGIC Result
# MAGIC
# MAGIC If you got here with no assertion errors, Auto Loader works against UC Volumes on this workspace and the framework's core assumption holds. Continue with 01_load_control_tables.py.

# COMMAND ----------

dbutils.notebook.exit("OK autoloader-on-volumes verified: 3 + 0 + 2 rows across three passes")