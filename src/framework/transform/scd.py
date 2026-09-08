"""Delta writers: append, overwrite, delete_insert, SCD Type-1 and SCD Type-2.

Shared by the silver and gold runners - the loading pattern is a property of the
target, not of the layer, so a gold dimension and a silver table use the same code.

SCD2 is implemented as the standard two-step merge, because a single MERGE statement
cannot both close the old version and insert the new one for the same key:

  step 1  close the current version whose record_hash differs, and simultaneously
          insert the new version for keys that are genuinely new;
  step 2  insert the new version for the keys just closed.

Rows are identified by business_keys; change is detected on record_hash, computed over
scd2_hash_columns (or every non-key, non-audit column when that is not specified).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ..exceptions import UnsupportedLoadTypeError
from ..logging_utils import FrameworkLogger
from ..spark_utils import (
    apply_liquid_clustering,
    apply_table_properties,
    ensure_schema,
    fq,
    hash_columns,
    table_exists,
)

RECORD_START_TS = "record_start_ts"
RECORD_END_TS = "record_end_ts"
RECORD_IS_ACTIVE = "record_is_active"
RECORD_HASH = "record_hash"
SCD2_COLUMNS = (RECORD_START_TS, RECORD_END_TS, RECORD_IS_ACTIVE, RECORD_HASH)

# Framework-owned columns that must never take part in change detection: a new
# _ingest_ts on an otherwise identical row is not a business change.
_NON_BUSINESS_PREFIXES = ("_", "record_")


@dataclass
class WriteResult:
    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    total_written: int = 0


class DeltaWriter:
    def __init__(self, spark: SparkSession, logger: Optional[FrameworkLogger] = None):
        self.spark = spark
        self.log = logger or FrameworkLogger({"component": "delta_writer"})

    # =============================================================================
    # dispatch
    # =============================================================================
    def write(
        self,
        df: DataFrame,
        catalog: str,
        schema: str,
        table: str,
        load_type: str,
        business_keys: Optional[List[str]] = None,
        sequence_by: Optional[str] = None,
        hash_column_list: Optional[List[str]] = None,
        partition_columns: Optional[List[str]] = None,
        cluster_by: Optional[List[str]] = None,
        table_properties: Optional[Dict[str, str]] = None,
        deduplicate: bool = True,
    ) -> WriteResult:
        """Write `df` to the target using `load_type`."""
        ensure_schema(self.spark, catalog, schema)
        target = f"{catalog}.{schema}.{table}"
        business_keys = business_keys or []
        log = self.log.child(target=target, load_type=load_type)

        if load_type in {"scd1", "scd2", "delete_insert"} and deduplicate:
            df = self._deduplicate(df, business_keys, sequence_by, log)

        if load_type == "append":
            return self._append(df, target, partition_columns, cluster_by, table_properties)
        if load_type == "overwrite":
            return self._overwrite(df, target, partition_columns, cluster_by, table_properties)
        if load_type == "scd1":
            return self._scd1(df, catalog, schema, table, business_keys, partition_columns, cluster_by, table_properties)
        if load_type == "scd2":
            return self._scd2(
                df, catalog, schema, table, business_keys, hash_column_list, partition_columns, cluster_by, table_properties
            )
        if load_type == "delete_insert":
            return self._delete_insert(
                df, catalog, schema, table, business_keys, partition_columns, cluster_by, table_properties
            )
        raise UnsupportedLoadTypeError(
            f"{target}: load_type {load_type!r} is not implemented "
            f"(supported: append, overwrite, scd1, scd2, delete_insert)"
        )

    # =============================================================================
    # simple modes
    # =============================================================================
    def _append(
        self,
        df: DataFrame,
        target: str,
        partition_columns: Optional[List[str]],
        cluster_by: Optional[List[str]],
        table_properties: Optional[Dict[str, str]],
    ) -> WriteResult:
        count = df.count()
        created = not self.spark.catalog.tableExists(target)
        writer = df.write.format("delta").mode("append").option("mergeSchema", "true")
        if partition_columns and created:
            writer = writer.partitionBy(*partition_columns)
        writer.saveAsTable(target)
        if created:
            self._apply_layout(target, cluster_by, table_properties)
        return WriteResult(inserted=count, total_written=count)

    def _overwrite(
        self,
        df: DataFrame,
        target: str,
        partition_columns: Optional[List[str]],
        cluster_by: Optional[List[str]],
        table_properties: Optional[Dict[str, str]],
    ) -> WriteResult:
        count = df.count()
        created = not self.spark.catalog.tableExists(target)
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        writer.saveAsTable(target)
        if created:
            self._apply_layout(target, cluster_by, table_properties)
        return WriteResult(inserted=count, total_written=count)

    # =============================================================================
    # SCD Type-1
    # =============================================================================
    def _scd1(
        self,
        df: DataFrame,
        catalog: str,
        schema: str,
        table: str,
        business_keys: List[str],
        partition_columns: Optional[List[str]],
        cluster_by: Optional[List[str]],
        table_properties: Optional[Dict[str, str]],
    ) -> WriteResult:
        """Upsert on business keys, keeping only the latest version of each row."""
        target = f"{catalog}.{schema}.{table}"
        df = df.withColumn("_scd_updated_ts", F.current_timestamp())

        if not table_exists(self.spark, catalog, schema, table):
            self.log.info("target absent, seeding with the first batch", target=target)
            return self._append(df, target, partition_columns, cluster_by, table_properties)

        view = f"_etl_scd1_{table}"
        df.createOrReplaceTempView(view)
        on_clause = " AND ".join(f"t.`{k}` = s.`{k}`" for k in business_keys)
        result = self.spark.sql(
            f"""
            MERGE WITH SCHEMA EVOLUTION INTO {fq(catalog, schema, table)} t
            USING {view} s
              ON {on_clause}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        self.spark.catalog.dropTempView(view)
        counts = _merge_counts(result)
        return WriteResult(
            inserted=counts.get("num_inserted_rows", 0),
            updated=counts.get("num_updated_rows", 0),
            total_written=counts.get("num_inserted_rows", 0) + counts.get("num_updated_rows", 0),
        )

    # =============================================================================
    # SCD Type-2
    # =============================================================================
    def _scd2(
        self,
        df: DataFrame,
        catalog: str,
        schema: str,
        table: str,
        business_keys: List[str],
        hash_column_list: Optional[List[str]],
        partition_columns: Optional[List[str]],
        cluster_by: Optional[List[str]],
        table_properties: Optional[Dict[str, str]],
    ) -> WriteResult:
        """Close changed versions and open new ones, keyed on business_keys."""
        target_fq = fq(catalog, schema, table)
        hash_cols = self._resolve_hash_columns(df, business_keys, hash_column_list)
        self.log.debug("SCD2 change detection columns", target=f"{catalog}.{schema}.{table}", columns=hash_cols)

        staged = (
            df.withColumn(RECORD_HASH, hash_columns(hash_cols))
            .withColumn(RECORD_START_TS, F.current_timestamp())
            .withColumn(RECORD_END_TS, F.lit(None).cast("timestamp"))
            .withColumn(RECORD_IS_ACTIVE, F.lit(True))
        )

        if not table_exists(self.spark, catalog, schema, table):
            self.log.info("target absent, seeding all rows as the first version", target=f"{catalog}.{schema}.{table}")
            count = staged.count()
            writer = staged.write.format("delta").mode("append")
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            writer.saveAsTable(f"{catalog}.{schema}.{table}")
            self._apply_layout(f"{catalog}.{schema}.{table}", cluster_by, table_properties)
            return WriteResult(inserted=count, total_written=count)

        view = f"_etl_scd2_{table}"
        staged.createOrReplaceTempView(view)
        on_clause = " AND ".join(f"t.`{k}` = s.`{k}`" for k in business_keys)
        insert_columns = ", ".join(f"`{c}`" for c in staged.columns)
        insert_values = ", ".join(f"s.`{c}`" for c in staged.columns)

        # Step 1: close the superseded version. New keys are inserted in the same
        # statement so they are not revisited in step 2.
        step1 = self.spark.sql(
            f"""
            MERGE WITH SCHEMA EVOLUTION INTO {target_fq} t
            USING {view} s
              ON {on_clause}
             AND t.{RECORD_IS_ACTIVE} = true
            WHEN MATCHED AND t.{RECORD_HASH} <> s.{RECORD_HASH} THEN UPDATE SET
                 t.{RECORD_END_TS}    = s.{RECORD_START_TS},
                 t.{RECORD_IS_ACTIVE} = false
            WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
            """
        )
        step1_counts = _merge_counts(step1)

        # Step 2: open the new version for every key closed above. The join on
        # record_is_active = true finds no active row for those keys any more.
        step2 = self.spark.sql(
            f"""
            MERGE INTO {target_fq} t
            USING {view} s
              ON {on_clause}
             AND t.{RECORD_IS_ACTIVE} = true
            WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
            """
        )
        step2_counts = _merge_counts(step2)
        self.spark.catalog.dropTempView(view)

        inserted = step1_counts.get("num_inserted_rows", 0) + step2_counts.get("num_inserted_rows", 0)
        closed = step1_counts.get("num_updated_rows", 0)
        return WriteResult(inserted=inserted, updated=closed, deleted=closed, total_written=inserted)

    def _resolve_hash_columns(
        self, df: DataFrame, business_keys: List[str], hash_column_list: Optional[List[str]]
    ) -> List[str]:
        """Columns compared for SCD2 change detection.

        Defaults to every column that is neither a business key nor framework-owned,
        so adding a source column automatically joins change detection - the
        alternative silently ignores new attributes until someone updates metadata.
        """
        if hash_column_list:
            missing = [c for c in hash_column_list if c not in df.columns]
            if missing:
                raise UnsupportedLoadTypeError(
                    f"scd2_hash_columns {missing} are not present in the source "
                    f"(available: {sorted(df.columns)})"
                )
            return hash_column_list
        keys = {k.lower() for k in business_keys}
        return [
            c
            for c in df.columns
            if c.lower() not in keys and not c.lower().startswith(_NON_BUSINESS_PREFIXES)
        ]

    # =============================================================================
    # delete + insert
    # =============================================================================
    def _delete_insert(
        self,
        df: DataFrame,
        catalog: str,
        schema: str,
        table: str,
        business_keys: List[str],
        partition_columns: Optional[List[str]],
        cluster_by: Optional[List[str]],
        table_properties: Optional[Dict[str, str]],
    ) -> WriteResult:
        """Delete the incoming keys, then insert the batch.

        The pattern for a full-restatement feed: the source resends every row for a
        key set, and the target must not keep rows the source no longer sends.
        """
        target = f"{catalog}.{schema}.{table}"
        if not table_exists(self.spark, catalog, schema, table):
            return self._append(df, target, partition_columns, cluster_by, table_properties)

        view = f"_etl_di_{table}"
        df.createOrReplaceTempView(view)
        on_clause = " AND ".join(f"t.`{k}` = s.`{k}`" for k in business_keys)
        delete_result = self.spark.sql(
            f"""
            MERGE INTO {fq(catalog, schema, table)} t
            USING (SELECT DISTINCT {', '.join(f'`{k}`' for k in business_keys)} FROM {view}) s
              ON {on_clause}
            WHEN MATCHED THEN DELETE
            """
        )
        deleted = _merge_counts(delete_result).get("num_deleted_rows", 0)
        count = df.count()
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)
        self.spark.catalog.dropTempView(view)
        return WriteResult(inserted=count, deleted=deleted, total_written=count)

    # =============================================================================
    # helpers
    # =============================================================================
    def _deduplicate(
        self,
        df: DataFrame,
        business_keys: List[str],
        sequence_by: Optional[str],
        log: FrameworkLogger,
    ) -> DataFrame:
        """Reduce the batch to one row per business key.

        A merge fails outright when the source has two rows for the same key, so this
        is not optional hygiene - it is what makes a merge-based load survive a source
        that sends multiple changes per key in one extract.
        """
        if not business_keys:
            return df
        order_column = sequence_by if sequence_by and sequence_by in df.columns else None
        if order_column is None:
            for candidate in ("_ingest_ts", "_source_file_mod_time"):
                if candidate in df.columns:
                    order_column = candidate
                    break
        if order_column is None:
            log.warning(
                "no ordering column available for de-duplication - falling back to dropDuplicates, "
                "which picks an arbitrary row per key",
                business_keys=business_keys,
            )
            return df.dropDuplicates(business_keys)

        window = Window.partitionBy(*[F.col(k) for k in business_keys]).orderBy(
            F.col(order_column).desc_nulls_last()
        )
        return df.withColumn("_etl_rn", F.row_number().over(window)).filter(F.col("_etl_rn") == 1).drop("_etl_rn")

    def _apply_layout(
        self, target: str, cluster_by: Optional[List[str]], table_properties: Optional[Dict[str, str]]
    ) -> None:
        catalog, schema, table = target.split(".", 2)
        apply_liquid_clustering(self.spark, catalog, schema, table, cluster_by)
        apply_table_properties(self.spark, catalog, schema, table, table_properties)


def _merge_counts(result: DataFrame) -> Dict[str, int]:
    try:
        row = result.collect()[0].asDict()
        return {k: int(v or 0) for k, v in row.items() if k.startswith("num_")}
    except Exception:  # pragma: no cover
        return {}
