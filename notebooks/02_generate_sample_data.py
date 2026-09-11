# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02 - Generate sample landing data (Free Edition)
# MAGIC
# MAGIC Writes source files into the landing Volume so there is something to ingest. This
# MAGIC replaces whatever drops files into ADLS in a real deployment.
# MAGIC
# MAGIC The data is deliberately imperfect. Each bad row is engineered to trip exactly one
# MAGIC DQ rule assigned in `conf/metadata/dq_rule_assignment/crm.yml`, so a first run
# MAGIC visibly quarantines rows **without** failing the pipeline:
# MAGIC
# MAGIC | Row | Problem | Rule | Severity |
# MAGIC |---|---|---|---|
# MAGIC | customer 1004 | blank name | `DQ_NOT_BLANK` | drop |
# MAGIC | customer 1005 | `not-an-email` | `DQ_EMAIL_FORMAT` | warning |
# MAGIC | customer 1006 | 3-letter country | `DQ_LENGTH_EQUALS` | drop |
# MAGIC | customer 1007 | segment `PLATINUM` | `DQ_IN_LIST` | warning |
# MAGIC | customer 1008 | negative credit limit | `DQ_NON_NEGATIVE` | drop |
# MAGIC | order 2004 | negative amount | `DQ_POSITIVE` | drop |
# MAGIC | order 2005 | currency `JPY` | `DQ_IN_LIST` | drop |
# MAGIC | order 2006 | dated tomorrow | `DQ_NOT_FUTURE_DATED` | drop |
# MAGIC | order 2007 | DELIVERED, no delivery date | `DQ_NOT_NULL` | warning |
# MAGIC | order 2008 | delivered before ordered | `DQ_COMPARE_COLUMNS` | drop |
# MAGIC | order 2009 | customer 9999 does not exist | `DQ_REFERENCE_EXISTS` | warning |
# MAGIC
# MAGIC `customer_id` is unique and non-null throughout, so the two **fail**-severity rules
# MAGIC pass. See the README for how to trip one on purpose.
# MAGIC
# MAGIC ### Widgets
# MAGIC - `batch` — `1` writes the first extract. `2` writes a second extract that changes a
# MAGIC   customer's segment and restates an order, so you can watch SCD2 close a version
# MAGIC   and the bronze merge upsert a row.
# MAGIC - `reset` — `true` deletes the landing files first. It does **not** touch the
# MAGIC   checkpoints, so a reset alone will not cause re-ingestion; see the README.

# COMMAND ----------

import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent / "src"))

from framework.config import FrameworkConfig  # noqa: E402
from framework.logging_utils import FrameworkLogger  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("environment", "free", "Environment")
dbutils.widgets.dropdown("batch", "1", ["1", "2"], "Which extract to write")
dbutils.widgets.dropdown("reset", "false", ["true", "false"], "Delete landing files first")

environment = dbutils.widgets.get("environment").strip()
batch = dbutils.widgets.get("batch").strip()
reset = dbutils.widgets.get("reset") == "true"

cfg = FrameworkConfig.load(environment=environment)
log = FrameworkLogger({"notebook": "02_generate_sample_data", "environment": cfg.environment}, cfg.log_level)

free = cfg.free_edition
LANDING = (
    f"/Volumes/{cfg.framework_catalog}/"
    f"{free.get('volumes_schema', 'etl_volumes')}/"
    f"{free.get('landing_volume', 'landing')}"
)

CUSTOMER_DIR = f"{LANDING}/crm/customer"
ORDER_DIR = f"{LANDING}/crm/sales_order"
EVENT_DIR = f"{LANDING}/crm/interaction_event"

log.info("landing root resolved", landing=LANDING, batch=batch, reset=reset)

# COMMAND ----------

if reset:
    for directory in (CUSTOMER_DIR, ORDER_DIR, EVENT_DIR):
        try:
            dbutils.fs.rm(directory, recurse=True)
            log.info("landing directory removed", directory=directory)
        except Exception as exc:  # nothing there yet on a first run
            log.info("nothing to remove", directory=directory, detail=str(exc)[:120])

for directory in (CUSTOMER_DIR, ORDER_DIR, EVENT_DIR):
    dbutils.fs.mkdirs(directory)

# COMMAND ----------

# MAGIC %md
# MAGIC Customer - CSV
# MAGIC
# MAGIC Written with plain Python file I/O, which works on /Volumes/... paths from serverless compute.
# MAGIC
# MAGIC Row 1003 contains an address field that is quoted and includes both a comma and a newline. This is intentionally designed to validate the multiLine: true and escape settings configured in the Bronze control record.

# COMMAND ----------

from datetime import date, datetime, timedelta

TODAY = date.today()
NOW = datetime.now().replace(microsecond=0)

CUSTOMER_HEADER = (
    "customer_id,customer_name,email,country_code,customer_segment,"
    "credit_limit,address,created_at,last_modified_ts"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Batch 1 Customer Records

# COMMAND ----------

def customer_rows_batch_1():
    ts = NOW.isoformat(sep=" ")

    return [
        f'1001,Acme Industrial,ops@acme.example,GB,ENTERPRISE,850000.00,"1 Mill Lane, Leeds",2023-04-11 09:15:00,{ts}',
        f'1002,Bluefin Logistics,hello@bluefin.example,US,MID_MARKET,120000.00,"400 Harbor St",2023-06-02 14:02:00,{ts}',

        # Quoted field containing a comma and a newline
        f'1003,Corvus Retail,accounts@corvus.example,IN,SMB,45000.00,"12 Nehru Road,\nBengaluru 560001",2024-01-20 08:00:00,{ts}',

        # Blank name -> DQ_NOT_BLANK (drop)
        f'1004, ,void@nowhere.example,GB,SMB,15000.00,"7 Empty Row",2024-02-01 10:30:00,{ts}',

        # Malformed email -> DQ_EMAIL_FORMAT (warning), row still loads
        f'1005,Dunmore Foods,not-an-email,GB,SMB,32000.00,"9 Baker Way",2024-02-14 11:45:00,{ts}',

        # 3-letter country code -> DQ_LENGTH_EQUALS (drop)
        f'1006,Eastgate Media,team@eastgate.example,GBR,MID_MARKET,78000.00,"55 Print Row",2024-03-03 16:20:00,{ts}',

        # Unknown segment -> DQ_IN_LIST (warning), row still loads
        f'1007,Fairhaven Trust,info@fairhaven.example,US,PLATINUM,990000.00,"2 Trust Plaza",2024-03-19 12:00:00,{ts}',

        # Negative credit limit -> DQ_NON_NEGATIVE (drop)
        f'1008,Gridline Energy,ap@gridline.example,AU,ENTERPRISE,-5000.00,"18 Power St",2024-04-08 07:10:00,{ts}',

        f'1009,Harborview Clinic,admin@harborview.example,US,PUBLIC_SECTOR,60000.00,"3 Dockside Ave",2024-05-22 13:35:00,{ts}',
        f'1010,Ironvale Steel,buyer@ironvale.example,IN,ENTERPRISE,1500000.00,"88 Forge Rd",2024-07-30 09:00:00,{ts}',
    ]

# COMMAND ----------

# MAGIC %md
# MAGIC Batch 2 Customer Records
# MAGIC
# MAGIC A later extract where:
# MAGIC
# MAGIC Customer 1002 is promoted from MID_MARKET to ENTERPRISE.
# MAGIC Customer 1011 is a new customer.
# MAGIC
# MAGIC Re-running the pipeline with this extract should cause:
# MAGIC
# MAGIC An SCD Type 2 close and reopen for customer 1002.
# MAGIC A new current record for customer 1011.
# MAGIC No new versions for unchanged customers.

# COMMAND ----------

def customer_rows_batch_2():
    ts = (NOW + timedelta(hours=1)).isoformat(sep=" ")

    return [
        f'1002,Bluefin Logistics,hello@bluefin.example,US,ENTERPRISE,400000.00,"400 Harbor St",2023-06-02 14:02:00,{ts}',
        f'1011,Junction Rail,contracts@junction.example,GB,PUBLIC_SECTOR,210000.00,"1 Sidings Way",2025-01-15 10:00:00,{ts}',
    ]

# COMMAND ----------

#Write the Customer CSV File

rows = (
    customer_rows_batch_1()
    if batch == "1"
    else customer_rows_batch_2()
)

customer_file = f"{CUSTOMER_DIR}/customer_batch{batch}.csv"

with open(customer_file, "w", encoding="utf-8", newline="\n") as handle:
    handle.write(CUSTOMER_HEADER + "\n")
    handle.write("\n".join(rows) + "\n")

log.info(
    "customer CSV written",
    file=customer_file,
    row_count=len(rows),
)

# COMMAND ----------

# MAGIC %md
# MAGIC Sales Order - JSON Lines
# MAGIC
# MAGIC Each line in the file contains a single JSON object. Therefore, the Bronze control row is configured with multiLine: false.

# COMMAND ----------

#Batch 1 Sales Order Records

def order_rows_batch_1():
    ts = NOW.isoformat(sep=" ")
    d = lambda days: (TODAY - timedelta(days=days)).isoformat()  # noqa: E731

    return [
        {
            "order_id": 2001,
            "customer_id": 1001,
            "order_status": "DELIVERED",
            "order_date": d(9),
            "delivery_date": d(6),
            "order_amount": 12500.00,
            "currency_code": "GBP",
            "last_modified_ts": ts,
        },
        {
            "order_id": 2002,
            "customer_id": 1002,
            "order_status": "CONFIRMED",
            "order_date": d(7),
            "delivery_date": None,
            "order_amount": 3400.50,
            "currency_code": "USD",
            "last_modified_ts": ts,
        },
        {
            "order_id": 2003,
            "customer_id": 1003,
            "order_status": "CANCELLED",
            "order_date": d(6),
            "delivery_date": None,
            "order_amount": 890.00,
            "currency_code": "INR",
            "last_modified_ts": ts,
        },

        # Negative amount -> DQ_POSITIVE (drop)
        {
            "order_id": 2004,
            "customer_id": 1001,
            "order_status": "NEW",
            "order_date": d(5),
            "delivery_date": None,
            "order_amount": -220.00,
            "currency_code": "GBP",
            "last_modified_ts": ts,
        },

        # Unlisted currency -> DQ_IN_LIST (drop)
        {
            "order_id": 2005,
            "customer_id": 1009,
            "order_status": "SHIPPED",
            "order_date": d(4),
            "delivery_date": None,
            "order_amount": 7700.00,
            "currency_code": "JPY",
            "last_modified_ts": ts,
        },

        # Dated tomorrow -> DQ_NOT_FUTURE_DATED (drop)
        {
            "order_id": 2006,
            "customer_id": 1010,
            "order_status": "NEW",
            "order_date": (TODAY + timedelta(days=1)).isoformat(),
            "delivery_date": None,
            "order_amount": 5100.00,
            "currency_code": "EUR",
            "last_modified_ts": ts,
        },

        # DELIVERED with no delivery_date -> DQ_NOT_NULL,
        # filtered to delivered (warning)
        {
            "order_id": 2007,
            "customer_id": 1002,
            "order_status": "DELIVERED",
            "order_date": d(3),
            "delivery_date": None,
            "order_amount": 2300.00,
            "currency_code": "USD",
            "last_modified_ts": ts,
        },

        # Delivered before ordered -> DQ_COMPARE_COLUMNS (drop)
        {
            "order_id": 2008,
            "customer_id": 1003,
            "order_status": "DELIVERED",
            "order_date": d(2),
            "delivery_date": d(5),
            "order_amount": 1450.00,
            "currency_code": "INR",
            "last_modified_ts": ts,
        },

        # Customer 9999 is not in the customer feed
        # -> DQ_REFERENCE_EXISTS (warning)
        {
            "order_id": 2009,
            "customer_id": 9999,
            "order_status": "NEW",
            "order_date": d(1),
            "delivery_date": None,
            "order_amount": 640.00,
            "currency_code": "GBP",
            "last_modified_ts": ts,
        },

        # DRAFT: removed by the silver row's filter_condition,
        # not by DQ
        {
            "order_id": 2010,
            "customer_id": 1010,
            "order_status": "DRAFT",
            "order_date": d(1),
            "delivery_date": None,
            "order_amount": 999.00,
            "currency_code": "GBP",
            "last_modified_ts": ts,
        },
    ]

# COMMAND ----------

# MAGIC %md
# MAGIC Batch 2 Sales Order Records
# MAGIC
# MAGIC This extract restates order 2002 as DELIVERED with a later last_modified_ts.
# MAGIC
# MAGIC Because the Bronze control row uses:
# MAGIC
# MAGIC write_mode: merge
# MAGIC merge_keys: order_id
# MAGIC sequence_by: last_modified_ts
# MAGIC
# MAGIC the newer version replaces the earlier record instead of creating a duplicate.

# COMMAND ----------

def order_rows_batch_2():
    ts = (NOW + timedelta(hours=1)).isoformat(sep=" ")

    return [
        {
            "order_id": 2002,
            "customer_id": 1002,
            "order_status": "DELIVERED",
            "order_date": (TODAY - timedelta(days=7)).isoformat(),
            "delivery_date": (TODAY - timedelta(days=1)).isoformat(),
            "order_amount": 3400.50,
            "currency_code": "USD",
            "last_modified_ts": ts,
        },
        {
            "order_id": 2011,
            "customer_id": 1011,
            "order_status": "NEW",
            "order_date": TODAY.isoformat(),
            "delivery_date": None,
            "order_amount": 18250.00,
            "currency_code": "GBP",
            "last_modified_ts": ts,
        },
    ]

# COMMAND ----------

#Write the JSON Lines File

orders = (
    order_rows_batch_1()
    if batch == "1"
    else order_rows_batch_2()
)

order_file = f"{ORDER_DIR}/sales_order_batch{batch}.json"

with open(order_file, "w", encoding="utf-8", newline="\n") as handle:
    for order in orders:
        handle.write(json.dumps(order) + "\n")

log.info(
    "sales_order JSON written",
    file=order_file,
    row_count=len(orders),
)

# COMMAND ----------

# MAGIC %md
# MAGIC Interaction Event - Parquet
# MAGIC
# MAGIC Unlike CSV and JSON files, Parquet is a binary columnar format, so it is generated using Spark rather than manually writing file contents.
# MAGIC
# MAGIC The Bronze control row reads this location using:

# COMMAND ----------

source_file_type: parquet
#Create Sample Interaction Events

from pyspark.sql import Row  # noqa: E402

event_seed = [
    (1001, "CALL", 0),
    (1001, "EMAIL", 1),
    (1002, "WEB_VISIT", 1),
    (1003, "CHAT", 2),
    (1009, "MEETING", 2),
    (1010, "EMAIL", 3),
    (1002, "CALL", 3),
    (1010, "WEB_VISIT", 4),

    # Unlisted event type -> DQ_IN_LIST (warning)
    (1001, "CARRIER_PIGEON", 4),
]

offset = 0 if batch == "1" else 100

events = [
    Row(
        event_id=f"EV{offset + index:05d}",
        customer_id=customer_id,
        event_type=event_type,
        event_ts=NOW - timedelta(days=days_ago, minutes=index * 7),
        event_date=TODAY - timedelta(days=days_ago),
        channel="inbound" if index % 2 == 0 else "outbound",
    )
    for index, (customer_id, event_type, days_ago)
    in enumerate(event_seed)
]

# COMMAND ----------

#Write the Parquet File

#A single Parquet file is generated and appended to the target event directory.

(
    spark.createDataFrame(events)
    .repartition(1)
    .write
    .mode("append")
    .parquet(EVENT_DIR)
)
#Log the Result
log.info(
    "interaction_event parquet written",
    directory=EVENT_DIR,
    row_count=len(events),
)



# COMMAND ----------

# MAGIC %md
# MAGIC Expected Data Quality Scenarios
# MAGIC CALL, EMAIL, WEB_VISIT, CHAT, and MEETING are valid event types.
# MAGIC CARRIER_PIGEON is intentionally included to trigger DQ_IN_LIST as a warning.
# MAGIC Batch 2 uses an offset of 100 so that new event IDs are generated and do not collide with Batch 1 records.

# COMMAND ----------

#What landed

for label, directory in (
    ("customer", CUSTOMER_DIR),
    ("sales_order", ORDER_DIR),
    ("interaction_event", EVENT_DIR),
):
    files = dbutils.fs.ls(directory)

    print(f"\n{label} ({len(files)} file(s) in {directory})")

    for f in files:
        print(f"  {f.name:<45} {f.size:>10,} bytes")

# COMMAND ----------

# Read the CSV back with no options, to show what Auto Loader will be handed.
display(spark.read.text(CUSTOMER_DIR).limit(15))

# COMMAND ----------

dbutils.notebook.exit(
    f"OK batch={batch} customers={len(rows)} orders={len(orders)} events={len(events)} landing={LANDING}"
)