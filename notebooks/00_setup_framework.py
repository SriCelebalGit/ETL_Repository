# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Framework setup (Free Edition)
# MAGIC
# MAGIC Provisions everything the framework needs, entirely from `conf/framework.<env>.yml`.
# MAGIC Nothing is hard-coded to a particular catalog — change the YAML and re-run.
# MAGIC
# MAGIC In order:
# MAGIC
# MAGIC 1. **Catalogs** — every distinct catalog named by `framework_catalog` and
# MAGIC    `catalogs.*`. Created if missing, reused if present.
# MAGIC 2. **Control and audit schemas + tables** — from `ddl/*.sql`.
# MAGIC 3. **Volumes** — `landing` for source files, `checkpoints` for Auto Loader's schema
# MAGIC    store and streaming state.
# MAGIC 4. **Layer schemas** — each created in its own layer's catalog.
# MAGIC
# MAGIC Works for either layout: all three layer tokens pointing at one catalog (the safe
# MAGIC Free Edition default), or a separate catalog per layer.
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
create_catalogs = bool(free.get("create_catalogs", True))
volumes_catalog = free.get("volumes_catalog") or cfg.framework_catalog
volumes_schema = free.get("volumes_schema", "etl_volumes")
landing_volume = free.get("landing_volume", "landing")
checkpoint_volume = free.get("checkpoint_volume", "checkpoints")

# layer_schemas maps a logical layer to the schemas to create in THAT layer's catalog.
# Accepts a bare list as well, in which case everything goes in the framework catalog.
raw_layer_schemas = free.get("layer_schemas", {})
if isinstance(raw_layer_schemas, list):
    layer_schemas = {"__framework__": list(raw_layer_schemas)}
else:
    layer_schemas = {k: list(v or []) for k, v in dict(raw_layer_schemas).items()}

log.info(
    "resolved configuration",
    framework_catalog=cfg.framework_catalog,
    control_schema=cfg.control_schema,
    audit_schema=cfg.audit_schema,
    layer_catalogs=cfg.catalogs,
    checkpoint_root=cfg.checkpoint_root,
    create_catalogs=create_catalogs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Catalogs
# MAGIC
# MAGIC The set of catalogs is derived from the config, de-duplicated — so a single-catalog
# MAGIC layout creates one, and a catalog-per-layer layout creates up to four.
# MAGIC
# MAGIC `CREATE CATALOG` can be refused on a Free Edition workspace. Rather than let that
# MAGIC surface as an opaque failure 40 statements later, each attempt is checked: if the
# MAGIC catalog exists afterwards we continue, and if not the error explains the fallback.


def list_catalogs() -> set:
    """Catalog names visible to this principal. Read positionally - the column name
    of SHOW CATALOGS has differed between runtimes."""
    return {row[0] for row in spark.sql("SHOW CATALOGS").collect()}


# framework_catalog first: it holds the control tables everything else depends on.
required_catalogs = [cfg.framework_catalog]
for catalog in cfg.catalogs.values():
    if catalog not in required_catalogs:
        required_catalogs.append(catalog)
if volumes_catalog not in required_catalogs:
    required_catalogs.append(volumes_catalog)

existing = list_catalogs()
log.info("catalogs already visible", catalogs=sorted(existing))

created, reused, failed = [], [], {}

for catalog in required_catalogs:
    if catalog in existing:
        reused.append(catalog)
        log.info("catalog already exists", catalog=catalog)
        continue

    if not create_catalogs:
        failed[catalog] = "create_catalogs is false in the config and the catalog does not exist"
        continue

    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
        spark.sql(
            f"COMMENT ON CATALOG `{catalog}` IS "
            f"'Created by the metadata driven ETL framework setup ({cfg.environment})'"
        )
        created.append(catalog)
        log.info("catalog created", catalog=catalog)
    except Exception as exc:
        # Re-check: another task in the same job may have created it concurrently.
        if catalog in list_catalogs():
            reused.append(catalog)
            log.info("catalog appeared concurrently", catalog=catalog)
        else:
            failed[catalog] = str(exc)[:400]
            log.error("catalog creation failed", catalog=catalog, detail=str(exc)[:400])

# COMMAND ----------

if failed:
    detail = "\n".join(f"  - {c}: {reason}" for c, reason in failed.items())
    raise PermissionError(
        f"Could not create or find {len(failed)} catalog(s):\n{detail}\n\n"
        f"Catalogs available to you: {sorted(list_catalogs())}\n\n"
        f"Two ways forward:\n"
        f"  1. Point conf/framework.{environment}.yml at a catalog you already have - "
        f"set framework_catalog and all three entries under catalogs: to it. The layers "
        f"stay separated by schema, so a single catalog is a supported layout.\n"
        f"  2. Create the catalog through Catalog Explorer, then re-run this notebook."
    )

log.info("catalogs ready", created=created, reused=reused)
print(f"created: {created or 'none'}")
print(f"reused:  {reused or 'none'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Control and audit tables
# MAGIC
# MAGIC `split_sql_statements` splits on top-level semicolons only — the DDL's `COMMENT`
# MAGIC literals contain semicolons, which a naive `split(";")` would treat as statement
# MAGIC boundaries. The DDL creates its own schemas.


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
# MAGIC ## 3 · Volumes
# MAGIC
# MAGIC `landing` stands in for the ADLS container a paid workspace would use — point the
# MAGIC bronze control rows at paths beneath it.
# MAGIC
# MAGIC `checkpoints` holds Auto Loader's schema store and streaming checkpoints. Treat it
# MAGIC as data, not cache: deleting a feed's directory under it makes that feed re-ingest
# MAGIC its entire history.

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{volumes_catalog}`.`{volumes_schema}`")

volume_paths = {}
for volume in (landing_volume, checkpoint_volume):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{volumes_catalog}`.`{volumes_schema}`.`{volume}`")
    path = f"/Volumes/{volumes_catalog}/{volumes_schema}/{volume}"
    volume_paths[volume] = path
    log.info("volume ready", volume=f"{volumes_catalog}.{volumes_schema}.{volume}", path=path)

print(f"landing     {volume_paths[landing_volume]}")
print(f"checkpoints {volume_paths[checkpoint_volume]}")

# COMMAND ----------

# The config's checkpoint_root must actually sit inside the checkpoint volume just
# created. If it does not, Auto Loader writes its state to a path with no volume behind
# it - which fails at the first micro-batch, or worse, silently loses the checkpoint.
expected_root = volume_paths[checkpoint_volume]
if not cfg.checkpoint_root.rstrip("/").startswith(expected_root):
    raise ValueError(
        f"checkpoint_root is {cfg.checkpoint_root!r} but the checkpoint volume created here is "
        f"{expected_root!r}.\n\n"
        f"Fix conf/framework.{environment}.yml so they agree, e.g.\n"
        f"  checkpoint_root: {expected_root}"
    )
log.info("checkpoint_root verified", checkpoint_root=cfg.checkpoint_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Layer schemas
# MAGIC
# MAGIC Each schema is created in the catalog its own layer resolves to. When all three
# MAGIC layer tokens point at one catalog, distinct schema names are what keep bronze and
# MAGIC silver from resolving to the same table — so this step also checks for that.

created_schemas = []
for layer, schemas in layer_schemas.items():
    if layer == "__framework__":
        catalog = cfg.framework_catalog
    else:
        catalog = cfg.resolve_catalog(layer)
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        created_schemas.append(f"{catalog}.{schema}")
        log.info("layer schema ready", layer=layer, schema=f"{catalog}.{schema}")

for entry in created_schemas:
    print(entry)

# COMMAND ----------

# A layer schema colliding across layers within one catalog is the failure mode that
# would make a silver load overwrite its own bronze source. Catch it at setup.
collisions = {}
for layer, schemas in layer_schemas.items():
    if layer == "__framework__":
        continue
    catalog = cfg.resolve_catalog(layer)
    for schema in schemas:
        collisions.setdefault(f"{catalog}.{schema}", []).append(layer)

shared = {ns: layers for ns, layers in collisions.items() if len(set(layers)) > 1}
if shared:
    detail = "\n".join(f"  - {ns} is used by layers {sorted(set(l))}" for ns, l in shared.items())
    raise ValueError(
        f"The same catalog.schema is assigned to more than one layer:\n{detail}\n\n"
        f"With a shared namespace, a bronze table and a silver table of the same name are "
        f"the SAME table, and the silver load would overwrite its own source. Give each "
        f"layer its own schema (or its own catalog) in conf/framework.{environment}.yml."
    )
log.info("no layer namespace collisions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Cross-check the DQ reference tables
# MAGIC
# MAGIC `dq_rules_assignment.rule_parameters.reference_table` is a literal three-part name —
# MAGIC the one place in the metadata that names a physical catalog. If you changed the
# MAGIC catalog in the config but not there, the referential check fails at silver load
# MAGIC time with a table-not-found. Warn about it now instead.

import re  # noqa: E402

known_catalogs = {cfg.framework_catalog, *cfg.catalogs.values()}
assignment_dir = repo_root / "conf" / "metadata" / "dq_rule_assignment"
mismatched = []

for yml in sorted(assignment_dir.glob("*.y*ml")) if assignment_dir.exists() else []:
    for match in re.finditer(r"reference_table:\s*([A-Za-z0-9_.`-]+)", yml.read_text(encoding="utf-8")):
        reference = match.group(1).replace("`", "")
        catalog = reference.split(".")[0]
        if catalog not in known_catalogs:
            mismatched.append((yml.name, reference, catalog))

if mismatched:
    print("WARNING - reference_table values naming a catalog that is not in this config:\n")
    for file_name, reference, catalog in mismatched:
        print(f"  {file_name}: {reference}   (catalog {catalog!r} is not one of {sorted(known_catalogs)})")
    print(
        f"\nEither this reference genuinely lives elsewhere, or it needs updating to one of "
        f"{sorted(known_catalogs)}. The silver load will fail on a table-not-found if it is wrong."
    )
    log.warning("dq reference_table catalog mismatch", count=len(mismatched))
else:
    log.info("dq reference_table catalogs all match the config")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

display(
    spark.sql(
        f"""
        SELECT table_schema, table_name, table_type
        FROM `{cfg.framework_catalog}`.information_schema.tables
        WHERE table_schema IN ('{cfg.control_schema}', '{cfg.audit_schema}')
        ORDER BY table_schema, table_name
        """
    )
)

# COMMAND ----------

# information_schema.volumes is not exposed on every workspace, so this is best effort.
try:
    display(
        spark.sql(
            f"""
            SELECT volume_catalog, volume_schema, volume_name, volume_type
            FROM `{volumes_catalog}`.information_schema.volumes
            WHERE volume_schema = '{volumes_schema}'
            ORDER BY volume_name
            """
        )
    )
except Exception as exc:
    print(f"information_schema.volumes unavailable ({str(exc)[:160]}) - listing paths instead:")
    for name, path in volume_paths.items():
        print(f"  {name:<12} {path}")

# COMMAND ----------

log.info("setup complete", next_step="notebooks/03_smoke_test_autoloader.py")

dbutils.notebook.exit(
    f"OK catalogs_created={created or 'none'} catalogs_reused={reused or 'none'} "
    f"landing={volume_paths[landing_volume]} checkpoints={cfg.checkpoint_root} "
    f"schemas={len(created_schemas)}"
)
