"""Spark and Databricks helpers shared by every layer."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from typing import Any, Dict, Iterable, List, Optional

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

_ILLEGAL_COL_CHARS = re.compile(r"[ ,;{}()\n\t=\.\-/]+")
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


# ---------------------------------------------------------------------------------
# session
# ---------------------------------------------------------------------------------
def get_spark() -> SparkSession:
    """Return the active session, creating one only when running outside Databricks."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName("etl_framework").getOrCreate()
    return spark


def get_dbutils(spark: Optional[SparkSession] = None):
    """Return dbutils, or None when running off-cluster (e.g. under pytest)."""
    try:  # available in notebooks and on serverless
        import IPython  # noqa: WPS433

        shell = IPython.get_ipython()
        if shell is not None and "dbutils" in shell.user_ns:
            return shell.user_ns["dbutils"]
    except Exception:  # pragma: no cover - IPython absent in unit tests
        pass
    try:
        from pyspark.dbutils import DBUtils  # noqa: WPS433

        return DBUtils(spark or get_spark())
    except Exception:  # pragma: no cover
        return None


# ---------------------------------------------------------------------------------
# identifiers
# ---------------------------------------------------------------------------------
def fq(catalog: str, schema: str, table: str) -> str:
    """Fully qualified, back-quoted Unity Catalog name."""
    return f"`{catalog}`.`{schema}`.`{table}`"


def fq_plain(catalog: str, schema: str, table: str) -> str:
    """Fully qualified name without back-quotes, for logging and audit columns."""
    return f"{catalog}.{schema}.{table}"


def new_uuid() -> str:
    return str(uuid.uuid4())


def normalise_column_name(name: str) -> str:
    """Make a source column name safe for Delta and predictable for downstream SQL.

    Splits camelCase, lower-cases, and collapses characters Delta cannot store in a
    column name into single underscores.
    """
    stepped = _CAMEL_BOUNDARY.sub("_", name.strip())
    cleaned = _ILLEGAL_COL_CHARS.sub("_", stepped)
    cleaned = re.sub(r"[^0-9a-zA-Z_]", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_").lower()
    return cleaned or "unnamed_column"


def normalise_columns(df: DataFrame, skip_prefix: str = "_") -> DataFrame:
    """Rename every column via normalise_column_name.

    Framework audit columns (leading underscore) and the rescued data column are left
    alone so downstream code can rely on their exact names.
    """
    renamed: Dict[str, str] = {}
    for original in df.columns:
        if original.startswith(skip_prefix):
            continue
        target = normalise_column_name(original)
        if target != original:
            renamed[original] = target
    for original, target in renamed.items():
        df = df.withColumnRenamed(original, target)
    return df


# ---------------------------------------------------------------------------------
# catalog / table inspection
# ---------------------------------------------------------------------------------
def table_exists(spark: SparkSession, catalog: str, schema: str, table: str) -> bool:
    return spark.catalog.tableExists(f"{catalog}.{schema}.{table}")


def ensure_schema(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")


def row_count(spark: SparkSession, catalog: str, schema: str, table: str) -> int:
    if not table_exists(spark, catalog, schema, table):
        return 0
    return spark.sql(f"SELECT COUNT(*) AS c FROM {fq(catalog, schema, table)}").collect()[0]["c"]


def apply_table_properties(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    properties: Optional[Dict[str, str]],
) -> None:
    if not properties:
        return
    clause = ", ".join(f"'{k}' = '{v}'" for k, v in properties.items())
    spark.sql(f"ALTER TABLE {fq(catalog, schema, table)} SET TBLPROPERTIES ({clause})")


def apply_liquid_clustering(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    cluster_by: Optional[Iterable[str]],
) -> None:
    """Apply CLUSTER BY to an existing table.

    Liquid clustering is preferred over partitioning for bronze/silver: it avoids the
    small-file explosion that low-cardinality partition columns cause on streaming
    ingestion, and it can be changed later without rewriting the table.
    """
    cols = [c for c in (cluster_by or []) if c]
    if not cols:
        return
    col_list = ", ".join(f"`{c}`" for c in cols)
    spark.sql(f"ALTER TABLE {fq(catalog, schema, table)} CLUSTER BY ({col_list})")


# ---------------------------------------------------------------------------------
# hashing
# ---------------------------------------------------------------------------------
def sha256_of_payload(payload: Dict[str, Any]) -> str:
    """Stable SHA-256 over a metadata payload, used for SCD2 change detection.

    Sorting keys and normalising to JSON means a reordered YAML file does not look
    like a change, which would otherwise close and reopen every control row on
    every CI run.
    """
    canonical = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def hash_columns(columns: List[str]) -> Column:
    """SHA-256 column expression over `columns`, NULL-safe.

    NULLs are mapped to a sentinel so that (NULL, 'a') and ('a', NULL) hash
    differently - concat_ws alone would collapse them.
    """
    if not columns:
        return F.lit(None).cast("string")
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in columns]
    return F.sha2(F.concat_ws("||", *parts), 256)


# ---------------------------------------------------------------------------------
# job context
# ---------------------------------------------------------------------------------
def job_context(spark: Optional[SparkSession] = None) -> Dict[str, Optional[str]]:
    """Best-effort workflow identifiers for the audit tables.

    Task values are only present when the notebook runs as a job task; interactive
    runs get a synthetic batch id so ad hoc testing still produces audit rows.
    """
    spark = spark or get_spark()
    ctx: Dict[str, Optional[str]] = {
        "job_id": None,
        "job_run_id": None,
        "task_run_id": None,
        "task_name": None,
        "job_name": None,
        "cluster_id": None,
        "run_by": None,
    }
    dbutils = get_dbutils(spark)
    if dbutils is not None:
        try:
            raw = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            ctx["job_id"] = _scala_opt(raw.tags().get("jobId"))
            ctx["job_run_id"] = _scala_opt(raw.tags().get("multitaskParentRunId")) or _scala_opt(
                raw.tags().get("jobRunId")
            )
            ctx["task_run_id"] = _scala_opt(raw.tags().get("runId"))
            ctx["task_name"] = _scala_opt(raw.tags().get("taskKey"))
            ctx["job_name"] = _scala_opt(raw.tags().get("jobName"))
            ctx["cluster_id"] = _scala_opt(raw.tags().get("clusterId"))
            ctx["run_by"] = _scala_opt(raw.tags().get("user"))
        except Exception:  # pragma: no cover - tag layout varies by runtime
            pass
    if not ctx["run_by"]:
        try:
            ctx["run_by"] = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
        except Exception:  # pragma: no cover
            ctx["run_by"] = None
    return ctx


def _scala_opt(value: Any) -> Optional[str]:
    """Unwrap a Scala Option returned through py4j."""
    if value is None:
        return None
    try:
        if hasattr(value, "isDefined"):
            return str(value.get()) if value.isDefined() else None
    except Exception:  # pragma: no cover
        return None
    return str(value)
