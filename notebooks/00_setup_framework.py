# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Framework setup (Free Edition)
# MAGIC
# MAGIC Creates everything the framework needs inside a single Unity Catalog catalog:
# MAGIC
# MAGIC 1. the control and audit schemas, and their tables, from `ddl/`
# MAGIC 2. two **UC Volumes** — `landing` for source files and `checkpoints` for Auto
# MAGIC    Loader's schema store and streaming state
# MAGIC 3. the per-layer schemas (`bronze_crm`, `silver_crm`, `gold_sales`)
# MAGIC
# MAGIC It does **not** create catalogs. Free Edition gives you one workspace catalog, so
# MAGIC `conf/framework.free.yml` must name a catalog that already exists.
# MAGIC
# MAGIC Idempotent — safe to re-run.

# COMMAND ----------

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent / "src"))

from framework.config import FrameworkConfig  # noqa: E402
from framework.logging_utils import FrameworkLogger  # noqa: E402
from framework.runtime import ensure_repo_on_path  # noqa: E402
from framework.sql_utils import render_placeholders, split_sql_statements  # noqa: E402

repo_root = ensure_repo_on_path()

# COMMAND ----------

dbutils.widgets.text("environment", "free", "Environment")
environment = dbutils.widgets.get("environment").strip()

cfg = FrameworkConfig.load(environment=environment)
log = FrameworkLogger({"notebook": "00_setup_framework", "environment": cfg.environment}, cfg.log_level)

free = cfg.free_edition
volumes_schema = free.get("volumes_schema", "etl_volumes")
landing_volume = free.get("landing_volume", "landing")
checkpoint_volume = free.get("checkpoint_volume", "checkpoints")
layer_schemas = free.get("layer_schemas", [])

log.info(
    "resolved configuration",
    framework_catalog=cfg.framework_catalog,
    control_schema=cfg.control_schema,
    audit_schema=cfg.audit_schema,
    checkpoint_root=cfg.checkpoint_root,
    catalogs=cfg.catalogs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confirm the catalog exists
# MAGIC
# MAGIC This is the single most common Free Edition setup failure: the catalog in the config
# MAGIC does not match the one the workspace actually has. Fail here with a readable message
# MAGIC rather than 40 statements later.

catalogs_present = {r["catalog"] for r in spark.sql("SHOW CATALOGS").collect()}
log.info("catalogs visible in this workspace", catalogs=sorted(catalogs_present))

required_catalogs = {cfg.framework_catalog} | set(cfg.catalogs.values())
missing = sorted(c for c in required_catalogs if c not in catalogs_present)

if missing:
    raise ValueError(
        f"Catalog(s) {missing} do not exist in this workspace. "
        f"Available: {sorted(catalogs_present)}. "
        f"Free Edition does not reliably allow CREATE CATALOG, so edit "
        f"conf/framework.{environment}.yml to use one of the available catalogs "
        f"(replace every occurrence of 'workspace')."
    )

log.info("catalog check passed", catalogs=sorted(required_catalogs))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control and audit tables

def run_ddl(path: Path) -> int:
    script = render_placeholders(
        path.read_text(encoding="utf-8"),
        {
            "fw_catalog": cfg.framework_catalog,
            "fw_schema": cfg.control_schema,
            "fw_audit_schema": cfg.audit_schema,
        },
    )
    statements = split_sql_statements(script)
    for statement in statements:
        spark.sql(statement)
    log.info("DDL applied", file=path.name, statement_count=len(statements))
    return len(statements)


total = 0
for ddl_file in sorted((repo_root / "ddl").glob("*.sql")):
    total += run_ddl(ddl_file)

log.info("DDL complete", statements_executed=total)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volumes
# MAGIC
# MAGIC `landing` stands in for the ADLS container that a paid workspace would use. Point
# MAGIC the bronze control rows at paths beneath it.
# MAGIC
# MAGIC `checkpoints` holds Auto Loader's schema store and streaming checkpoints. Treat it
# MAGIC as data: deleting a feed's directory under it makes that feed re-ingest everything.

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cfg.framework_catalog}`.`{volumes_schema}`")

for volume in (landing_volume, checkpoint_volume):
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS `{cfg.framework_catalog}`.`{volumes_schema}`.`{volume}`"
    )
    log.info(
        "volume ready",
        volume=f"{cfg.framework_catalog}.{volumes_schema}.{volume}",
        path=f"/Volumes/{cfg.framework_catalog}/{volumes_schema}/{volume}",
    )

# The checkpoint_root in the config must actually resolve to the volume just created.
expected_root = f"/Volumes/{cfg.framework_catalog}/{volumes_schema}/{checkpoint_volume}"
if not cfg.checkpoint_root.rstrip("/").startswith(expected_root):
    raise ValueError(
        f"checkpoint_root is {cfg.checkpoint_root!r} but the checkpoint volume created here is "
        f"{expected_root!r}. These must agree, or Auto Loader will write its state somewhere "
        f"that is not backed by the volume. Fix conf/framework.{environment}.yml."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Layer schemas
# MAGIC
# MAGIC All three layers share one catalog on Free Edition, so they are separated by schema.
# MAGIC Creating them up front means the first pipeline run cannot fail on a missing
# MAGIC namespace.

for schema in layer_schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cfg.framework_catalog}`.`{schema}`")
    log.info("layer schema ready", schema=f"{cfg.framework_catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

display(
    spark.sql(
        f"""
        SELECT table_schema, table_name, table_type
        FROM {cfg.framework_catalog}.information_schema.tables
        WHERE table_schema IN ('{cfg.control_schema}', '{cfg.audit_schema}')
        ORDER BY table_schema, table_name
        """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT volume_schema, volume_name, volume_type, storage_location
        FROM {cfg.framework_catalog}.information_schema.volumes
        WHERE volume_schema = '{volumes_schema}'
        ORDER BY volume_name
        """
    )
)

# COMMAND ----------

log.info("setup complete - next run notebooks/02_generate_sample_data.py")
dbutils.notebook.exit(
    f"OK catalog={cfg.framework_catalog} landing=/Volumes/{cfg.framework_catalog}/"
    f"{volumes_schema}/{landing_volume} checkpoints={cfg.checkpoint_root}"
)
