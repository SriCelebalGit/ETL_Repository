"""Tests for the gold transformation modules.

These pass only because a transformation is a pure function of (spark, ctx) that
returns a DataFrame instead of writing one - which is the reason the framework offers
`module` alongside the design's `notebook`.
"""

from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402


class FakeContext:
    """Stands in for framework.gold.TransformContext with in-memory tables."""

    def __init__(self, spark, silver_tables=None, gold_tables=None, parameters=None):
        self.spark = spark
        self._silver = silver_tables or {}
        self._gold = gold_tables or {}
        self.parameters = parameters or {}
        self.batch_id = "test-batch"
        self.target_catalog = "gold_test"
        self.target_schema = "sales"
        self.target_table = "target"

    def silver(self, table, schema=None):
        return self._silver[table]

    def gold(self, table, schema=None):
        return self._gold[table]

    def param(self, key, default=None):
        return self.parameters.get(key, default)

    def int_param(self, key, default):
        value = self.parameters.get(key)
        return int(value) if value is not None else default


# =====================================================================================
# dim_customer
# =====================================================================================
@pytest.fixture()
def silver_customer(spark):
    return spark.createDataFrame(
        [
            (1, "Alice", "a@x.com", "GB", "SMB", 5_000.0, True),
            (2, "Bob", "b@x.com", "US", None, 250_000.0, True),
            (3, "Carol", "c@x.com", "IN", "SMB", None, True),
            # An expired SCD2 version that must not reach the dimension.
            (1, "Alice Old", "a@x.com", "GB", "SMB", 1_000.0, False),
        ],
        "customer_id int, customer_name string, email string, country_code string, "
        "customer_segment string, credit_limit double, record_is_active boolean",
    ).withColumn("source_created_at", F.current_timestamp()).withColumn(
        "source_updated_at", F.current_timestamp()
    )


def test_dim_customer_uses_only_the_active_version(spark, silver_customer):
    from transformations.gold import dim_customer

    ctx = FakeContext(spark, silver_tables={"customer": silver_customer})
    result = dim_customer.transform(spark, ctx).collect()

    assert len(result) == 3
    assert "Alice Old" not in [r["customer_name"] for r in result]


def test_dim_customer_bands_credit_and_defaults_the_segment(spark, silver_customer):
    from transformations.gold import dim_customer

    ctx = FakeContext(
        spark, silver_tables={"customer": silver_customer}, parameters={"default_segment": "UNKNOWN"}
    )
    rows = {r["customer_id"]: r for r in dim_customer.transform(spark, ctx).collect()}

    assert rows[1]["credit_band"] == "LOW"        # 5,000
    assert rows[2]["credit_band"] == "HIGH"       # 250,000
    assert rows[3]["credit_band"] == "UNKNOWN"    # null credit limit
    assert rows[2]["customer_segment"] == "UNKNOWN"  # null segment defaulted
    assert rows[1]["customer_segment"] == "SMB"


# =====================================================================================
# dim_date
# =====================================================================================
def test_dim_date_covers_the_requested_range(spark):
    from transformations.gold import dim_date

    ctx = FakeContext(spark, parameters={"start_date": "2024-01-01", "end_date": "2024-01-31"})
    result = dim_date.transform(spark, ctx)
    assert result.count() == 31


def test_dim_date_fiscal_year_starts_in_the_configured_month(spark):
    from transformations.gold import dim_date

    ctx = FakeContext(
        spark,
        parameters={
            "start_date": "2024-03-01",
            "end_date": "2024-04-30",
            "fiscal_year_start_month": "4",
        },
    )
    rows = {str(r["date_key"]): r for r in dim_date.transform(spark, ctx).collect()}

    march = rows["2024-03-31"]
    april = rows["2024-04-01"]
    # With an April start, March belongs to the previous fiscal year - the off-by-one
    # every hand-rolled date dimension gets wrong.
    assert march["fiscal_year"] == 2023
    assert march["fiscal_month"] == 12
    assert april["fiscal_year"] == 2024
    assert april["fiscal_month"] == 1
    assert april["fiscal_quarter"] == 1


def test_dim_date_surrogate_key_is_the_yyyymmdd_integer(spark):
    from transformations.gold import dim_date

    ctx = FakeContext(spark, parameters={"start_date": "2024-06-15", "end_date": "2024-06-15"})
    row = dim_date.transform(spark, ctx).collect()[0]
    assert row["date_sk"] == 20240615
    assert row["is_weekend"] is True  # 15 June 2024 was a Saturday


# =====================================================================================
# fct_sales
# =====================================================================================
def test_fct_sales_zeroes_cancelled_revenue_but_keeps_the_row(spark):
    from transformations.gold import fct_sales

    orders = spark.createDataFrame(
        [
            (101, 1, "CONFIRMED", datetime(2024, 6, 10).date(), 500.0, "GBP"),
            (102, 1, "CANCELLED", datetime(2024, 6, 10).date(), 900.0, "GBP"),
        ],
        "order_id int, customer_id int, order_status string, order_date date, "
        "order_amount double, currency_code string",
    ).withColumn("source_updated_at", F.current_timestamp())

    dim_customer = spark.createDataFrame(
        [(1, "SMB", "LOW", "GB", datetime(2020, 1, 1), None)],
        "customer_id int, customer_segment string, credit_band string, country_code string, "
        "record_start_ts timestamp, record_end_ts timestamp",
    )
    dim_date = spark.createDataFrame(
        [(datetime(2024, 6, 10).date(), 20240610)], "date_key date, date_sk int"
    )

    ctx = FakeContext(
        spark,
        silver_tables={"sales_order": orders},
        gold_tables={"dim_customer": dim_customer, "dim_date": dim_date},
        parameters={"lookback_days": "10000"},  # wide window so the fixture dates are in range
    )
    rows = {r["order_id"]: r for r in fct_sales.transform(spark, ctx).collect()}

    assert len(rows) == 2  # the cancelled order is retained for volume reporting
    assert float(rows[101]["net_order_amount"]) == 500.0
    assert float(rows[102]["net_order_amount"]) == 0.0
    assert float(rows[102]["order_amount"]) == 900.0  # the gross amount is preserved


def test_fct_sales_joins_the_scd2_version_current_at_the_order_date(spark):
    from transformations.gold import fct_sales

    orders = spark.createDataFrame(
        [
            (201, 1, "CONFIRMED", datetime(2024, 1, 15).date(), 100.0, "GBP"),
            (202, 1, "CONFIRMED", datetime(2024, 8, 15).date(), 100.0, "GBP"),
        ],
        "order_id int, customer_id int, order_status string, order_date date, "
        "order_amount double, currency_code string",
    ).withColumn("source_updated_at", F.current_timestamp())

    # The customer moved from SMB to ENTERPRISE on 1 June 2024.
    dim_customer = spark.createDataFrame(
        [
            (1, "SMB", "LOW", "GB", datetime(2020, 1, 1), datetime(2024, 6, 1)),
            (1, "ENTERPRISE", "HIGH", "GB", datetime(2024, 6, 1), None),
        ],
        "customer_id int, customer_segment string, credit_band string, country_code string, "
        "record_start_ts timestamp, record_end_ts timestamp",
    )
    dim_date = spark.createDataFrame(
        [
            (datetime(2024, 1, 15).date(), 20240115),
            (datetime(2024, 8, 15).date(), 20240815),
        ],
        "date_key date, date_sk int",
    )

    ctx = FakeContext(
        spark,
        silver_tables={"sales_order": orders},
        gold_tables={"dim_customer": dim_customer, "dim_date": dim_date},
        parameters={"lookback_days": "10000"},
    )
    rows = {r["order_id"]: r for r in fct_sales.transform(spark, ctx).collect()}

    # Historical sales must carry the attributes that were current then, not today's.
    assert rows[201]["customer_segment"] == "SMB"
    assert rows[202]["customer_segment"] == "ENTERPRISE"
