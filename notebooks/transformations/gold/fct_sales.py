"""Gold transformation: fct_sales.

Restates a rolling window of days on every run (load_type: delete_insert on order_id),
which is how late-arriving corrections land without rewriting the whole fact table.

Dimension keys are resolved by joining to the dimensions rather than recomputing them,
so a fact row always points at the surrogate key the dimension actually holds.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def transform(spark: SparkSession, ctx) -> DataFrame:
    lookback_days = ctx.int_param("lookback_days", 7)

    orders = ctx.silver("sales_order").filter(
        F.col("order_date") >= F.date_sub(F.current_date(), lookback_days)
    )

    # The dimension is SCD2, so the join must pick the version that was current when
    # the order was placed - joining on the active version alone would attribute
    # historical sales to today's customer attributes.
    dim_customer = ctx.gold("dim_customer").select(
        F.col("customer_id").alias("dim_customer_id"),
        F.col("customer_segment"),
        F.col("credit_band"),
        F.col("country_code"),
        F.col("record_start_ts"),
        F.col("record_end_ts"),
    )

    dim_date = ctx.gold("dim_date").select(F.col("date_key"), F.col("date_sk"))

    enriched = (
        orders.join(
            dim_customer,
            (orders["customer_id"] == dim_customer["dim_customer_id"])
            & (orders["order_date"].cast("timestamp") >= dim_customer["record_start_ts"])
            & (
                dim_customer["record_end_ts"].isNull()
                | (orders["order_date"].cast("timestamp") < dim_customer["record_end_ts"])
            ),
            how="left",
        )
        .join(dim_date, orders["order_date"] == dim_date["date_key"], how="left")
        .drop("dim_customer_id", "record_start_ts", "record_end_ts", "date_key")
    )

    return enriched.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("date_sk").alias("order_date_sk"),
        F.col("order_date"),
        F.col("order_status"),
        F.col("currency_code"),
        F.col("order_amount"),
        # A cancelled order stays in the fact table for volume reporting, but must not
        # contribute to revenue - so the measure is zeroed rather than the row dropped.
        F.when(F.col("order_status") == "CANCELLED", F.lit(0).cast("decimal(18,2)"))
        .otherwise(F.col("order_amount"))
        .alias("net_order_amount"),
        F.col("customer_segment"),
        F.col("credit_band"),
        F.col("country_code"),
        F.col("source_updated_at"),
        F.current_timestamp().alias("_loaded_at"),
        F.lit(ctx.batch_id).alias("_batch_id"),
    )
