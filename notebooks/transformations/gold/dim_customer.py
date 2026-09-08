"""Gold transformation: dim_customer.

Reads the SCD2 silver customer table and produces the current-version dimension rows.
The framework applies SCD2 again on the gold side (load_type: scd2 in the control row),
which is what keeps the dimension's surrogate keys stable for facts that already point
at a historical version.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def transform(spark: SparkSession, ctx) -> DataFrame:
    default_segment = ctx.param("default_segment", "UNKNOWN")

    # Silver holds SCD2 history; the dimension build works from the current version and
    # lets the gold-side SCD2 merge decide what constitutes a new version.
    customer = ctx.silver("customer").filter(F.col("record_is_active"))

    return customer.select(
        F.col("customer_id"),
        F.col("customer_name"),
        F.col("email"),
        F.col("country_code"),
        F.coalesce(F.col("customer_segment"), F.lit(default_segment)).alias("customer_segment"),
        _credit_band(F.col("credit_limit")).alias("credit_band"),
        F.col("credit_limit"),
        F.col("source_created_at"),
        F.col("source_updated_at"),
        F.lit(ctx.batch_id).alias("_batch_id"),
    )


def _credit_band(credit_limit):
    """Band the credit limit for reporting.

    Kept as a named function so the boundaries are visible in one place and testable
    without a Spark session's worth of setup around them.
    """
    return (
        F.when(credit_limit.isNull(), F.lit("UNKNOWN"))
        .when(credit_limit < 10_000, F.lit("LOW"))
        .when(credit_limit < 100_000, F.lit("MEDIUM"))
        .when(credit_limit < 1_000_000, F.lit("HIGH"))
        .otherwise(F.lit("STRATEGIC"))
    )
