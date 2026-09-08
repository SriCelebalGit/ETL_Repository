"""Registry of python DQ rule functions (rule_type = 'function').

A rule function receives the DataFrame under test plus the target column and the
rule's parameters, and returns a boolean Column that is TRUE for rows that PASS.
Returning a Column rather than a filtered DataFrame is what lets the engine evaluate
every rule in a single pass over the data.

SQL expression rules cover most checks; a function is the right tool when the check
needs a window, a lookup against another table, or logic that does not read well as a
one-line SQL clause.

Register a new rule with the @dq_function decorator, then reference it from
conf/metadata/dq_rules/*.yml as `rule_type: function` / `rule: <name>`.
"""

from __future__ import annotations

from typing import Callable, Dict, Mapping

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RuleFunction = Callable[[DataFrame, str, Mapping[str, str]], Column]

_REGISTRY: Dict[str, RuleFunction] = {}


def dq_function(name: str) -> Callable[[RuleFunction], RuleFunction]:
    def decorator(func: RuleFunction) -> RuleFunction:
        if name in _REGISTRY:
            raise ValueError(f"DQ function {name!r} is already registered")
        _REGISTRY[name] = func
        return func

    return decorator


def get_function(name: str) -> RuleFunction:
    try:
        return _REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"DQ function {name!r} is not registered. Available: {sorted(_REGISTRY)}. "
            f"Add it with @dq_function in framework/dq/functions.py."
        ) from exc


def registered_functions() -> list:
    return sorted(_REGISTRY)


# =====================================================================================
# built-in rule functions
# =====================================================================================
@dq_function("is_unique")
def is_unique(df: DataFrame, column: str, params: Mapping[str, str]) -> Column:
    """TRUE only for rows whose `column` value appears exactly once in the batch.

    Uniqueness cannot be expressed as a row-local SQL clause, which is exactly why it
    belongs here. `scope` optionally makes the check unique-within-group, e.g.
    scope: region_code for a per-region invoice number.
    """
    scope = [c.strip() for c in str(params.get("scope", "")).split(",") if c.strip()]
    window = Window.partitionBy(*(scope + [column])) if scope else Window.partitionBy(column)
    return F.count(F.lit(1)).over(window) == 1


@dq_function("is_not_future_dated")
def is_not_future_dated(df: DataFrame, column: str, params: Mapping[str, str]) -> Column:
    """TRUE when `column` is not later than now plus an optional tolerance.

    `tolerance_days` allows for clock skew between the source system and the platform.
    """
    tolerance_days = int(params.get("tolerance_days", 0))
    upper_bound = F.date_add(F.current_date(), tolerance_days)
    return F.col(column).isNull() | (F.to_date(F.col(column)) <= upper_bound)


@dq_function("is_within_stddev")
def is_within_stddev(df: DataFrame, column: str, params: Mapping[str, str]) -> Column:
    """TRUE when `column` sits within N standard deviations of the batch mean.

    A statistical outlier guard for measures where a fixed min/max would be wrong as
    volumes grow. Pair it with severity: warning - an outlier is a signal, not proof
    the row is invalid.
    """
    sigmas = float(params.get("sigmas", 4))
    stats = Window.partitionBy()
    mean = F.avg(F.col(column).cast("double")).over(stats)
    stddev = F.stddev_pop(F.col(column).cast("double")).over(stats)
    return (
        F.col(column).isNull()
        | stddev.isNull()
        | (stddev == 0)
        | (F.abs(F.col(column).cast("double") - mean) <= sigmas * stddev)
    )


@dq_function("exists_in_reference")
def exists_in_reference(df: DataFrame, column: str, params: Mapping[str, str]) -> Column:
    """TRUE when `column` matches a value in a reference table.

    Referential integrity against a dimension or code list. Requires
    reference_table and reference_column parameters; the reference is read once and
    broadcast, so keep it to a code list rather than a fact table.
    """
    reference_table = params.get("reference_table")
    reference_column = params.get("reference_column")
    if not reference_table or not reference_column:
        raise ValueError(
            "exists_in_reference requires 'reference_table' and 'reference_column' parameters"
        )
    spark = df.sparkSession
    keys = (
        spark.table(reference_table)
        .select(F.col(reference_column).cast("string").alias("_ref_key"))
        .distinct()
    )
    collected = {row["_ref_key"] for row in keys.collect() if row["_ref_key"] is not None}
    if not collected:
        return F.col(column).isNull()
    return F.col(column).isNull() | F.col(column).cast("string").isin(list(collected))


@dq_function("matches_checksum")
def matches_checksum(df: DataFrame, column: str, params: Mapping[str, str]) -> Column:
    """TRUE when `column` equals the SHA-256 of the columns listed in `over`.

    Verifies that a source-supplied hash column agrees with the data that arrived -
    a cheap end-to-end integrity check on feeds that provide one.
    """
    over = [c.strip() for c in str(params.get("over", "")).split(",") if c.strip()]
    if not over:
        raise ValueError("matches_checksum requires an 'over' parameter listing the hashed columns")
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in over]
    return F.col(column).isNull() | (
        F.lower(F.col(column)) == F.sha2(F.concat_ws("||", *parts), 256)
    )
