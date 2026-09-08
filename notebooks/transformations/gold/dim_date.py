"""Gold transformation: dim_date.

Generated rather than sourced, so it is rebuilt with load_type: overwrite. The fiscal
calendar start month is a parameter because it differs per organisation and should not
require a code change.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def transform(spark: SparkSession, ctx) -> DataFrame:
    start_date = ctx.param("start_date", "2020-01-01")
    end_date = ctx.param("end_date", "2030-12-31")
    fiscal_start_month = ctx.int_param("fiscal_year_start_month", 1)

    dates = spark.sql(
        f"SELECT explode(sequence(DATE'{start_date}', DATE'{end_date}', INTERVAL 1 DAY)) AS date_key"
    )

    month = F.month("date_key")
    year = F.year("date_key")

    # A fiscal year starting in April means January-March belong to the previous
    # fiscal year, which is the off-by-one every hand-written date dimension gets wrong.
    fiscal_year = F.when(month >= fiscal_start_month, year).otherwise(year - 1)
    fiscal_month = F.when(
        month >= fiscal_start_month, month - fiscal_start_month + 1
    ).otherwise(month + 12 - fiscal_start_month + 1)

    return dates.select(
        F.date_format("date_key", "yyyyMMdd").cast("int").alias("date_sk"),
        F.col("date_key"),
        year.alias("calendar_year"),
        F.quarter("date_key").alias("calendar_quarter"),
        month.alias("calendar_month"),
        F.date_format("date_key", "MMMM").alias("calendar_month_name"),
        F.weekofyear("date_key").alias("calendar_week"),
        F.dayofmonth("date_key").alias("day_of_month"),
        F.dayofweek("date_key").alias("day_of_week"),
        F.date_format("date_key", "EEEE").alias("day_name"),
        F.dayofweek("date_key").isin(1, 7).alias("is_weekend"),
        F.last_day("date_key").alias("month_end_date"),
        (F.col("date_key") == F.last_day("date_key")).alias("is_month_end"),
        F.trunc("date_key", "month").alias("month_start_date"),
        fiscal_year.alias("fiscal_year"),
        fiscal_month.alias("fiscal_month"),
        F.ceil(fiscal_month / 3).cast("int").alias("fiscal_quarter"),
        F.lit(ctx.batch_id).alias("_batch_id"),
    )
