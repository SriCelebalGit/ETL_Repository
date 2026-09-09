# ETL Framework — Databricks Free Edition build

A self-contained variant of the framework in the parent folder, adapted to run on
**Databricks Free Edition**: UC Volumes instead of ADLS, serverless instead of job
clusters, and sample data generated in-workspace so there is something to ingest.

Nothing here reaches outside this folder. The parent build is untouched.

---

## One line controls every name

`conf/framework.free.yml` opens with a `vars:` block. **Change `catalog:` there and
everything follows** — notebook 00 creates it, and the metadata YAML picks it up:

```yaml
vars:
  catalog: etl_framework        # <- the only edit needed to rename everything
  control_schema: etl_control
  audit_schema: etl_audit
  volumes_schema: etl_volumes
  landing_volume: landing
  checkpoint_volume: checkpoints
  bronze_schema: bronze_crm
  silver_schema: silver_crm
  gold_schema: gold_sales

# derived - nothing below needs editing
framework_catalog: ${catalog}
catalogs:
  bronze: ${catalog}
  silver: ${catalog}
  gold: ${catalog}
checkpoint_root: /Volumes/${catalog}/${volumes_schema}/${checkpoint_volume}
```

Setting `catalog: etl_framework` gives you:

| | |
|---|---|
| Control tables | `etl_framework.etl_control.*` |
| Audit tables | `etl_framework.etl_audit.*` |
| Layer tables | `etl_framework.bronze_crm.*`, `.silver_crm.*`, `.gold_sales.*` |
| Landing files | `/Volumes/etl_framework/etl_volumes/landing/crm/…` |
| Checkpoints | `/Volumes/etl_framework/etl_volumes/checkpoints` |
| DQ reference table | `etl_framework.bronze_crm.customer` |

A var may reference another var (`volumes_root: /Volumes/${catalog}/${volumes_schema}`),
and `${env}` is always available (`catalog: etl_${env}` renders as `etl_free`). An unknown
or misspelled placeholder **fails at config load** with the valid names listed, rather
than creating a catalog literally named `${catlog}`. A cycle between two vars is reported
too.

The metadata never repeats a catalog name:

```yaml
# conf/metadata/bronze_control/crm.yml
file_location: ${landing_root}/crm/customer/          # -> /Volumes/etl_framework/etl_volumes/landing/crm/customer/
bronze_schema_name: ${bronze_schema}                  # -> bronze_crm

# conf/metadata/dq_rule_assignment/crm.yml
reference_table: ${bronze_catalog}.${bronze_schema}.customer   # -> etl_framework.bronze_crm.customer
```

Tokens available in any metadata YAML: everything you declared under `vars:`, plus the
derived `${landing_root}`, `${checkpoint_root}`, `${framework_catalog}`,
`${bronze_catalog}`, `${silver_catalog}`, `${gold_catalog}`, `${bronze_schema}`,
`${silver_schema}`, `${gold_schema}`, `${control_schema}`, `${audit_schema}` and
`${env}`. The derived ones win where a name appears in both. An unknown token fails the
metadata load with the valid names listed.

### Two supported layouts

**Layout A (shipped default)** — one catalog, a schema per layer. Safest on Free Edition,
since it needs at most one catalog created.

**Layout B** — a catalog per layer, closer to production. Add three more vars and point
the mapping at them:

```yaml
vars:
  bronze_catalog: etl_bronze
  silver_catalog: etl_silver
  gold_catalog: etl_gold

catalogs:
  bronze: ${bronze_catalog}
  silver: ${silver_catalog}
  gold: ${gold_catalog}
```

Notebook 00 creates all of them, and each layer schema is created in its own layer's
catalog. Nothing else changes, because control rows carry the logical tokens
`bronze` / `silver` / `gold` rather than catalog names.

> **Why the layer schemas are prefixed.** Under Layout A all three layers share one
> catalog, so if bronze and silver both used schema `crm`, then `crm.customer` would be
> the *same table* in both layers and the silver load would overwrite its own source.
> Hence `bronze_crm` / `silver_crm` / `gold_sales`. Notebook 00 fails the setup if it
> detects two layers resolving to one namespace.

If your workspace refuses `CREATE CATALOG`, notebook 00 says so and tells you how to fall
back: set `catalog:` to one you already have — Free Edition usually ships `workspace` —
and re-run. Still one line, and no metadata edits.

---

## Run it

Import this folder into your Free Edition workspace as a Git folder (or upload it), then
run the notebooks in order. **Running them by hand is the fastest way to see it work** —
the bundle is optional and covered further down.

| # | Notebook | What it does | Run it |
|---|---|---|---|
| 0 | `00_setup_framework.py` | Creates the catalogs, control/audit schemas and tables, the two Volumes, and the layer schemas | once |
| 1 | `03_smoke_test_autoloader.py` | **Run this next.** Proves Auto Loader can read from *and checkpoint to* a Volume | once |
| 2 | `02_generate_sample_data.py` | Writes sample CSV / JSON / Parquet into the landing Volume | `batch=1` |
| 3 | `01_load_control_tables.py` | Loads the metadata YAML into the control tables | once per metadata change |
| 4 | `10_bronze_loader.py` | Ingest one bronze table | ×3, see below |
| 5 | `20_silver_loader.py` | DQ + SCD one silver table | ×3 |
| 6 | `30_gold_loader.py` | Build one gold object | ×4 |

### The loader widgets

Each loader takes the target's identity. The `catalog_name` widget accepts the **logical
token**, so these values are the same under either layout:

```
10_bronze_loader   catalog_name=bronze   schema_name=bronze_crm   table_name=customer
10_bronze_loader   catalog_name=bronze   schema_name=bronze_crm   table_name=sales_order
10_bronze_loader   catalog_name=bronze   schema_name=bronze_crm   table_name=interaction_event

20_silver_loader   catalog_name=silver   schema_name=silver_crm   table_name=customer
20_silver_loader   catalog_name=silver   schema_name=silver_crm   table_name=sales_order
20_silver_loader   catalog_name=silver   schema_name=silver_crm   table_name=interaction_event

30_gold_loader     catalog_name=gold     schema_name=gold_sales   table_name=dim_customer
30_gold_loader     catalog_name=gold     schema_name=gold_sales   table_name=dim_date
30_gold_loader     catalog_name=gold     schema_name=gold_sales   table_name=fct_sales
30_gold_loader     catalog_name=gold     schema_name=gold_sales   table_name=agg_daily_sales_by_segment
```

Leave `batch_id` blank when running by hand — a `manual-…` id is generated and threaded
through the audit tables.

Order matters twice: **all bronze before any silver** (the `sales_order` referential DQ
check reads the bronze customer table), and **dimensions before `fct_sales`** (its
`depends_on` is checked before it runs).

---

## What you should see

Queries below assume the shipped `etl_framework`; substitute your catalog if you changed
it.

`customer` has 10 source rows. Three trip `drop` rules, two trip `warning` rules:

```sql
-- 7 rows load (10 minus the 3 dropped); the 2 warnings are among them
SELECT count(*) FROM etl_framework.silver_crm.customer WHERE record_is_active;

-- 5 rows quarantined: 3 FAILED (excluded) + 2 WARNING (also loaded)
SELECT customer_id, _dq_status, _dq_failed_rules
FROM etl_framework.silver_crm.customer_quarantine;
```

Per-rule detail, which is the thing worth looking at:

```sql
SELECT table_name, column_name, rule_id, severity,
       rows_evaluated, rows_failed, pass_pct, rule_status
FROM etl_framework.etl_audit.dq_result_detail
ORDER BY rows_failed DESC, table_name, column_name;
```

And the run itself:

```sql
SELECT layer, table_name, job_status, records_read, records_inserted,
       records_rejected, files_processed, duration_seconds
FROM etl_framework.etl_audit.job_run_audit
ORDER BY task_start_timestamp;
```

### Then watch SCD2 actually work

Run `02_generate_sample_data.py` again with **`batch=2`**, then re-run the bronze and
silver loaders for `customer` and `sales_order`. The second extract promotes customer 1002
from `MID_MARKET` to `ENTERPRISE` and adds 1011, and restates order 2002 as `DELIVERED`.

```sql
-- 1002 now has two versions: one closed, one active. The others have one each.
SELECT customer_id, customer_segment, record_start_ts, record_end_ts, record_is_active
FROM etl_framework.silver_crm.customer
WHERE customer_id IN (1002, 1011)
ORDER BY customer_id, record_start_ts;

-- order 2002 was UPSERTED in bronze, not appended: still exactly one row
SELECT order_id, order_status, count(*) OVER (PARTITION BY order_id) AS row_count
FROM etl_framework.bronze_crm.sales_order WHERE order_id = 2002;
```

The eight unchanged customers produce **no** new versions — that is `row_hash` change
detection doing its job.

---

## What changed from the parent build, and why

| # | Parent | Here | Reason |
|---|---|---|---|
| 1 | `checkpoint_root: abfss://…` | A UC Volume path, rejected at config load if it contains `://` | Free Edition has no storage credential |
| 2 | `file_location: abfss://…` hard-coded | `${landing_root}/crm/…` | Volumes, and no catalog name repeated in metadata |
| 3 | Three catalogs, fixed literals | One `vars.catalog` line, created by notebook 00 | Free Edition gives one catalog; `CREATE CATALOG` may be refused, and a rename should be one edit |
| 4 | Bronze schema `crm`, silver schema `crm` | `${bronze_schema}` / `${silver_schema}` → `bronze_crm` / `silver_crm` | **Load-bearing.** Under one catalog, identical schemas make bronze and silver the *same table* |
| 5 | `CREATE CATALOG` inside the DDL | Notebook 00 creates catalogs first, with a readable error if refused | The DDL cannot report a permission problem usefully |
| 6 | Job clusters, Photon, `spark_conf`, `num_workers` | Serverless (no compute block at all) | Free Edition is serverless-only; a task naming a job cluster is rejected at deploy |
| 7 | `bronze_streaming_job.yml`, one `load_type: stream` feed | Deleted; all feeds `batch` | A continuous query never terminates and would drain the serverless budget |
| 8 | `cloudFiles.useNotifications: true` on the event feed | Removed; directory listing only | File notification mode needs cloud queue resources Free Edition has no access to |
| 9 | Concurrency 8–16 | 2 | Limited serverless capacity; higher fan-out just queues |
| 10 | `run_as` service principal | Removed | Single-user workspace |
| 11 | Nightly schedule, unpaused in prd | No schedule | An unattended run would silently consume the budget |
| 12 | `azure-pipelines.yml` | Removed | Not useful for local testing |
| 13 | Nothing generating data | `02_generate_sample_data.py` | No external system drops files here |
| 14 | Nothing verifying the platform | `03_smoke_test_autoloader.py` | Volumes-as-checkpoint-store is the one assumption worth proving first |
| 15 | `dq_failure_threshold_pct: 2–5%` | `40%` | The sample set is 10 rows with 3 deliberately bad, so a realistic threshold would abort the demo |
| 16 | `ctx.silver("customer", schema="crm")` | `ctx.silver("customer")` | Reads `silver_schema` from config, so it works under either layout |
| 17 | Referential DQ check → a silver table | → the bronze table, via tokens | Bronze completes before any silver task starts; pointing at silver would race, because silver tables load concurrently |

### Three code changes

Everything else in `src/framework/` is byte-identical to the parent.

- **`config.py`** — gained a `vars:` block (names written once, expanded across the whole
  file, cross-referencing allowed, cycles and unknown tokens reported), a `free_edition`
  block (read only by notebook 00), and a guard that **rejects a `checkpoint_root`
  containing `://`**. Without that guard, an `abfss://` path copied in by mistake fails at
  the first micro-batch write rather than at config load.
- **`control/metadata_loader.py`** — expands `${token}` in every string of a metadata
  record, including inside nested maps and lists, so landing paths and reference tables
  follow the config. An unknown token fails the load with the valid names listed.
- **`conf/framework.free.yml`** replaces `framework.dev.yml` / `framework.prd.yml`.

---

## Using the bundle instead

Optional. Set your workspace host in `databricks.yml` first.

```bash
databricks bundle validate -t free
databricks bundle deploy   -t free

# setup + sample data + metadata load, in one job
databricks bundle run control_table_load_job -t free

# the whole pipeline: discover -> bronze -> silver -> gold -> summary
databricks bundle run lakehouse_master_job -t free
```

To load the second sample extract through the job:

```bash
databricks bundle run control_table_load_job -t free --params sample_data_batch=2
databricks bundle run lakehouse_master_job -t free
```

---

## Things that will trip you up

**Re-running ingests nothing, and that is correct.** The Auto Loader checkpoint remembers
which files it consumed. To genuinely start over you must delete the checkpoint *as well
as* the table — `02_generate_sample_data.py` with `reset=true` deletes landing files only:

```sql
DROP TABLE IF EXISTS etl_framework.bronze_crm.customer;
```
```python
dbutils.fs.rm(
    "/Volumes/etl_framework/etl_volumes/checkpoints/bronze/etl_framework/bronze_crm/customer",
    recurse=True,
)
```

**Renaming a catalog after a first run leaves the old data behind.** The metadata follows
the config, but existing tables, Volumes and checkpoints do not move. Either rename before
you start, or drop the old catalog's schemas afterwards.

**Don't set `checkpoint_location` by hand.** Paths are derived per table from
`checkpoint_root`; setting them manually is the only way to get two feeds sharing one
checkpoint, which corrupts both.

**Serverless has no `spark.conf` you can set.** If you adapt this and reach for one, most
are rejected on serverless. The framework needs none.

**The `fail` severity rules pass on purpose.** The sample `customer_id` values are unique
and non-null so the demo completes. To watch a `fail` rule abort a batch, add a duplicate
id to `customer_rows_batch_1()` in the generator and re-run — the silver task stops, loads
nothing, and the error names the rule.

---

## Tests

```bash
python -m pytest tests -q
```

**82 pass, 21 skip.** The skips need a live Spark session (Java 17). The 82 cover config
resolution, `vars` expansion, control-row validation, metadata parsing, metadata token
expansion, SQL splitting, and four consistency guards:

- the Free-Edition metadata YAML validates against the table specs
- the DDL column lists match `metadata_loader.TABLE_SPECS`
- no shipped metadata file contains an unresolved `${token}`
- the shipped `framework.free.yml` resolves fully, and its layer schemas do not collide

Sixteen tests are new versus the parent: the external-path guard, single-catalog
resolution, the `free_edition` config block, five covering metadata token expansion, and
eight covering the `vars` block — including that renaming `catalog:` alone moves the
control tables, the checkpoint root, the landing paths and the DQ reference table.

---

## Not verified on a live workspace

This build has not been run against Databricks Free Edition — it was written and tested
statically. Tests, linting and YAML parsing all pass, and the adaptations follow from
documented Free Edition constraints, but here is where to expect friction, in order of
likelihood:

1. **Auto Loader checkpointing to a Volume.** `03_smoke_test_autoloader.py` settles this
   in one minute with no framework code involved. Run it first.
2. **`CREATE CATALOG` permission.** Notebook 00 attempts it, verifies the result, and on
   failure tells you to reuse an existing catalog instead.
3. **`CREATE VOLUME` permission.** Should be fine in your own catalog; if not, create the
   two Volumes in Catalog Explorer and re-run notebook 00.
4. **`for_each` tasks on serverless**, if you use the bundle. Running the notebooks by
   hand avoids the question entirely.
