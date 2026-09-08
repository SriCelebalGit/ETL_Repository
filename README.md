# ETL Framework — Databricks Free Edition build

A self-contained variant of the framework in the parent folder, adapted to run on
**Databricks Free Edition**: UC Volumes instead of ADLS, serverless instead of job
clusters, one catalog instead of three, and sample data generated in-workspace so there
is something to ingest.

Nothing here reaches outside this folder. The parent build is untouched.

---

## Run it

Import this folder into your Free Edition workspace as a Git folder (or upload it), then
run the notebooks in order. **Running the notebooks by hand is the fastest way to see it
work** — the bundle is optional and covered further down.

| # | Notebook | What it does | Run it |
|---|---|---|---|
| 0 | `00_setup_framework.py` | Checks your catalog exists, creates the control/audit schemas and tables, creates the two Volumes, creates the layer schemas | once |
| 1 | `03_smoke_test_autoloader.py` | **Run this next.** Proves Auto Loader can read from and checkpoint to a Volume | once |
| 2 | `02_generate_sample_data.py` | Writes sample CSV / JSON / Parquet into the landing Volume | `batch=1` |
| 3 | `01_load_control_tables.py` | Loads the metadata YAML into the control tables | once per metadata change |
| 4 | `10_bronze_loader.py` | Ingest one bronze table | ×3, see below |
| 5 | `20_silver_loader.py` | DQ + SCD one silver table | ×3 |
| 6 | `30_gold_loader.py` | Build one gold object | ×4 |

**Before step 0**, confirm your catalog name:

```sql
SHOW CATALOGS;
```

Free Edition usually gives you `workspace`. If yours differs, replace every occurrence of
`workspace` in `conf/framework.free.yml` and in `conf/metadata/dq_rule_assignment/crm.yml`
(one `reference_table` literal). Notebook 00 fails with a readable message if they do not
match, rather than part-way through.

### The loader widgets

Each loader notebook takes the target's identity. Run them in this order:

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

`customer` has 10 source rows. Three trip `drop` rules, two trip `warning` rules:

```sql
-- 7 rows load (10 minus the 3 dropped); the 2 warnings are among them
SELECT count(*) FROM workspace.silver_crm.customer WHERE record_is_active;

-- 5 rows quarantined: 3 FAILED (excluded) + 2 WARNING (also loaded)
SELECT customer_id, _dq_status, _dq_failed_rules
FROM workspace.silver_crm.customer_quarantine;
```

Per-rule detail, which is the thing worth looking at:

```sql
SELECT table_name, column_name, rule_id, severity,
       rows_evaluated, rows_failed, pass_pct, rule_status
FROM workspace.etl_audit.dq_result_detail
ORDER BY rows_failed DESC, table_name, column_name;
```

And the run itself:

```sql
SELECT layer, table_name, job_status, records_read, records_inserted,
       records_rejected, files_processed, duration_seconds
FROM workspace.etl_audit.job_run_audit
ORDER BY task_start_timestamp;
```

### Then watch SCD2 actually work

Run `02_generate_sample_data.py` again with **`batch=2`**, then re-run the bronze and
silver loaders for `customer` and `sales_order`. The second extract promotes customer 1002
from `MID_MARKET` to `ENTERPRISE` and adds 1011, and restates order 2002 as `DELIVERED`.

```sql
-- 1002 now has two versions: one closed, one active. The other customers have one each.
SELECT customer_id, customer_segment, record_start_ts, record_end_ts, record_is_active
FROM workspace.silver_crm.customer
WHERE customer_id IN (1002, 1011)
ORDER BY customer_id, record_start_ts;

-- order 2002 was UPSERTED in bronze, not appended: still exactly one row
SELECT order_id, order_status, count(*) OVER (PARTITION BY order_id) AS row_count
FROM workspace.bronze_crm.sales_order WHERE order_id = 2002;
```

The eight unchanged customers produce **no** new versions — that is `row_hash` change
detection doing its job.

---

## What changed from the parent build, and why

| # | Parent | Here | Reason |
|---|---|---|---|
| 1 | `checkpoint_root: abfss://…` | `/Volumes/workspace/etl_volumes/checkpoints` | Free Edition has no storage credential |
| 2 | `file_location: abfss://…` | `/Volumes/workspace/etl_volumes/landing/crm/…` | same |
| 3 | Three catalogs (`edp_bronze_dev`, …) | One catalog, three schemas | Free Edition gives one catalog; `CREATE CATALOG` is not guaranteed |
| 4 | Bronze schema `crm`, silver schema `crm` | `bronze_crm`, `silver_crm`, `gold_sales` | **Load-bearing.** With one catalog, identical schemas would make bronze and silver the *same table* — silver would overwrite its own source |
| 5 | `CREATE CATALOG` in DDL + notebook 00 | Removed; notebook 00 verifies the catalog exists instead | Free Edition may refuse it |
| 6 | Job clusters, Photon, `spark_conf`, `num_workers` | Serverless (no compute block at all) | Free Edition is serverless-only; a task naming a job cluster is rejected at deploy |
| 7 | `bronze_streaming_job.yml`, one `load_type: stream` feed | Deleted; all feeds are `batch` | A continuous query never terminates and would drain the serverless budget |
| 8 | `cloudFiles.useNotifications: true` on the event feed | Removed; directory listing only | File notification mode needs cloud queue resources Free Edition has no access to |
| 9 | Concurrency 8–16 | 2 | Limited serverless capacity; higher fan-out just queues |
| 10 | `run_as` service principal | Removed | Single-user workspace |
| 11 | Nightly schedule, unpaused in prd | No schedule | An unattended nightly run would silently consume the budget |
| 12 | `azure-pipelines.yml` | Removed | Not useful for local testing |
| 13 | Nothing generating data | `02_generate_sample_data.py` | No external system drops files here |
| 14 | Nothing verifying the platform | `03_smoke_test_autoloader.py` | Volumes-as-checkpoint-store is the one assumption worth proving first |
| 15 | `dq_failure_threshold_pct: 2–5%` | `40%` | The sample set is 10 rows with 3 deliberately bad, so a realistic threshold would abort the demo |
| 16 | `ctx.silver("customer", schema="crm")` | `ctx.silver("customer")` | Reads `silver_schema` from config, so the transformation works in either layout |
| 17 | Referential DQ check → `edp_silver_dev.crm.customer` | → `workspace.bronze_crm.customer` | Bronze completes before any silver task starts; pointing at silver would race, because silver tables load concurrently |

### Two code changes

Everything else in `src/framework/` is byte-identical to the parent. Two files differ:

- **`config.py`** gained a `free_edition` block (read only by notebook 00, to create the
  Volumes and layer schemas) and a validation guard that **rejects a `checkpoint_root`
  containing `://`**. Without the guard, an `abfss://` path copied in by mistake fails at
  the first micro-batch write rather than at config load.
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

The master job's `for_each` fan-out means you do not enumerate tables — it reads them from
the control tables at run time.

To load the second sample extract through the job:

```bash
databricks bundle run control_table_load_job -t free --params sample_data_batch=2
databricks bundle run lakehouse_master_job -t free
```

---

## Things that will trip you up

**Re-running ingests nothing, and that is correct.** The Auto Loader checkpoint remembers
which files it consumed. To genuinely start over you must delete the checkpoint *as well
as* the table — `02_generate_sample_data.py --reset=true` deletes landing files only:

```sql
DROP TABLE IF EXISTS workspace.bronze_crm.customer;
```
```python
dbutils.fs.rm("/Volumes/workspace/etl_volumes/checkpoints/bronze/workspace/bronze_crm/customer", recurse=True)
```

**Don't point two feeds at one checkpoint.** Paths are derived per table from
`checkpoint_root`, so this only happens if you set `checkpoint_location` by hand. Don't.

**Serverless has no `spark.conf` you can set.** If you adapt this and reach for a Spark
conf, most are rejected on serverless. The framework does not need any.

**The `fail` severity rules pass on purpose.** The sample `customer_id` values are unique
and non-null so the demo completes. To watch a `fail` rule abort a batch, add a duplicate
id to `customer_rows_batch_1()` in the generator and re-run — the silver task stops and
loads nothing, and the error names the rule.

**Free Edition has usage limits.** The full pipeline is 10 short serverless tasks. Running
it repeatedly in a loop is the only way to feel the ceiling; normal testing is fine.

---

## Tests

```bash
python -m pytest tests -q
```

69 pass, 21 skip. The skips need a live Spark session (Java 17); the 69 cover config
resolution, control-row validation, metadata parsing, SQL splitting, and two consistency
guards — **including that the Free-Edition metadata YAML in `conf/metadata/` validates
against the table specs**, which is what caught the schema renames while this build was
being put together.

Three tests are new here: the external-path guard, the single-catalog resolution, and the
`free_edition` config block.

---

## Not verified on a live workspace

This build has not been run against Databricks Free Edition — it was written and tested
statically. The tests, linting and YAML parsing all pass, and the adaptations follow from
documented Free Edition constraints, but the following are the places to expect friction,
in order of likelihood:

1. **Auto Loader checkpointing to a Volume.** `03_smoke_test_autoloader.py` exists
   precisely to settle this in one minute. Run it first.
2. **Your catalog name.** Notebook 00 checks it and fails clearly.
3. **`CREATE VOLUME` permissions.** Should be fine in your own catalog; if not, create the
   Volumes through the Catalog Explorer UI and re-run notebook 00.
4. **`for_each` tasks on serverless**, if you use the bundle. Running the notebooks by
   hand avoids the question entirely.
