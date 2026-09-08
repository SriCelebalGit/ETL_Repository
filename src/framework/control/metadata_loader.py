"""YAML -> control table loader (the "control table load pipeline" of the design).

Application developers commit YAML under conf/metadata/<table>/; CI runs this loader,
which upserts the files into the control tables as SCD Type-2. The loader is the only
writer to the control tables, which is what keeps the metadata reproducible from git.

SCD2 is applied in three passes per table:

  1. close   - an active row whose row_hash no longer matches the YAML is end-dated.
  2. insert  - new and changed business keys get a fresh active version.
  3. prune   - (optional) an active row whose business key vanished from the YAML is
               end-dated. Scoped to the config_file_name values in this run so a
               partial load can never retire another team's feeds.

row_hash is a canonical SHA-256 over the attribute payload, so re-running CI without a
YAML change is a no-op instead of a new version per row per run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..config import FrameworkConfig
from ..exceptions import MetadataValidationError
from ..logging_utils import FrameworkLogger
from ..spark_utils import sha256_of_payload

_STR = StringType()
_ARR = ArrayType(StringType())
_MAP = MapType(StringType(), StringType())
_BOOL = BooleanType()
_INT = IntegerType()
_DBL = DoubleType()


@dataclass(frozen=True)
class TableSpec:
    """Declarative description of one control table for the generic loader."""

    table_name: str
    yaml_dir: str
    root_key: str
    business_keys: Tuple[str, ...]
    required: Tuple[str, ...]
    columns: Tuple[Tuple[str, Any], ...]

    @property
    def attribute_columns(self) -> List[str]:
        """Columns that participate in change detection (everything except SCD2 admin)."""
        admin = {"row_hash", "record_start_ts", "record_end_ts", "record_is_active", "created_by"}
        return [name for name, _ in self.columns if name not in admin]

    @property
    def insert_columns(self) -> List[str]:
        return [name for name, _ in self.columns]

    def struct(self) -> StructType:
        return StructType([StructField(name, dtype, True) for name, dtype in self.columns])


# =====================================================================================
# Table specifications
# =====================================================================================
_SCD2_TAIL: Tuple[Tuple[str, Any], ...] = (
    ("config_file_name", _STR),
    ("row_hash", _STR),
    ("record_start_ts", TimestampType()),
    ("record_end_ts", TimestampType()),
    ("record_is_active", _BOOL),
    ("created_by", _STR),
)

BRONZE_SPEC = TableSpec(
    table_name="bronze_control_table",
    yaml_dir="bronze_control",
    root_key="bronze_control",
    business_keys=("target_catalog_name", "bronze_schema_name", "bronze_table_name"),
    required=(
        "source_system",
        "source_file_type",
        "file_location",
        "target_catalog_name",
        "bronze_schema_name",
        "bronze_table_name",
        "load_type",
    ),
    columns=(
        ("source_system", _STR),
        ("source_entity_name", _STR),
        ("source_file_type", _STR),
        ("file_location", _STR),
        ("source_file_pattern", _STR),
        ("target_catalog_name", _STR),
        ("bronze_schema_name", _STR),
        ("bronze_table_name", _STR),
        ("load_type", _STR),
        ("trigger_mode", _STR),
        ("trigger_interval", _STR),
        ("schema_location", _STR),
        ("checkpoint_location", _STR),
        ("schema_evolution_mode", _STR),
        ("schema_hints", _STR),
        ("rescued_data_column", _STR),
        ("reader_options", _MAP),
        ("cloud_files_options", _MAP),
        ("max_files_per_trigger", _INT),
        ("max_bytes_per_trigger", _STR),
        ("write_mode", _STR),
        ("primary_keys", _ARR),
        ("sequence_by", _STR),
        ("partition_columns", _ARR),
        ("cluster_by", _ARR),
        ("table_properties", _MAP),
        ("add_ingestion_metadata", _BOOL),
        ("normalise_column_names", _BOOL),
        ("is_enabled", _BOOL),
    )
    + _SCD2_TAIL,
)

SILVER_SPEC = TableSpec(
    table_name="silver_control_table",
    yaml_dir="silver_control",
    root_key="silver_control",
    business_keys=("target_catalog_name", "silver_schema_name", "silver_table_name"),
    required=(
        "source_catalog_name",
        "source_schema_name",
        "source_table_name",
        "target_catalog_name",
        "silver_schema_name",
        "load_type",
        "business_keys",
    ),
    columns=(
        ("source_catalog_name", _STR),
        ("source_schema_name", _STR),
        ("source_table_name", _STR),
        ("target_catalog_name", _STR),
        ("silver_schema_name", _STR),
        ("silver_table_name", _STR),
        ("load_type", _STR),
        ("read_mode", _STR),
        ("business_keys", _ARR),
        ("sequence_by", _STR),
        ("scd2_hash_columns", _ARR),
        ("watermark_column", _STR),
        ("select_expressions", _ARR),
        ("filter_condition", _STR),
        ("drop_columns", _ARR),
        ("deduplicate", _BOOL),
        ("partition_columns", _ARR),
        ("cluster_by", _ARR),
        ("table_properties", _MAP),
        ("checkpoint_location", _STR),
        ("dq_enabled", _BOOL),
        ("quarantine_enabled", _BOOL),
        ("quarantine_table_name", _STR),
        ("dq_failure_threshold_pct", _DBL),
        ("is_enabled", _BOOL),
    )
    + _SCD2_TAIL,
)

DQ_RULES_SPEC = TableSpec(
    table_name="dq_rules",
    yaml_dir="dq_rules",
    root_key="dq_rules",
    business_keys=("rule_id",),
    required=("rule_id", "rule_type", "rule"),
    columns=(
        ("rule_id", _STR),
        ("rule_name", _STR),
        ("rule_type", _STR),
        ("rule", _STR),
        ("rule_parameters", _MAP),
        ("dq_dimension", _STR),
        ("default_severity", _STR),
        ("description", _STR),
        ("is_enabled", _BOOL),
    )
    + _SCD2_TAIL,
)

DQ_ASSIGNMENT_SPEC = TableSpec(
    table_name="dq_rules_assignment",
    yaml_dir="dq_rule_assignment",
    root_key="dq_rules_assignment",
    business_keys=("catalog_name", "schema_name", "table_name", "column_name", "rule_id"),
    required=("catalog_name", "schema_name", "table_name", "column_name", "rule_id", "severity"),
    columns=(
        ("catalog_name", _STR),
        ("schema_name", _STR),
        ("table_name", _STR),
        ("column_name", _STR),
        ("rule_id", _STR),
        ("severity", _STR),
        ("rule_parameters", _MAP),
        ("filter_condition", _STR),
        ("is_enabled", _BOOL),
    )
    + _SCD2_TAIL,
)

GOLD_SPEC = TableSpec(
    table_name="gold_control_table",
    yaml_dir="gold_control",
    root_key="gold_control",
    business_keys=("target_catalog_name", "target_schema_name", "table_name"),
    required=("target_catalog_name", "target_schema_name", "table_name", "transformation_type", "load_type"),
    columns=(
        ("target_catalog_name", _STR),
        ("target_schema_name", _STR),
        ("table_name", _STR),
        ("object_type", _STR),
        ("transformation_type", _STR),
        ("notebook_name", _STR),
        ("module_name", _STR),
        ("sql_file_name", _STR),
        ("load_type", _STR),
        ("business_keys", _ARR),
        ("sequence_by", _STR),
        ("scd2_hash_columns", _ARR),
        ("depends_on", _ARR),
        ("parameters", _MAP),
        ("partition_columns", _ARR),
        ("cluster_by", _ARR),
        ("table_properties", _MAP),
        ("is_enabled", _BOOL),
    )
    + _SCD2_TAIL,
)

TABLE_SPECS: Dict[str, TableSpec] = {
    spec.table_name: spec
    for spec in (BRONZE_SPEC, SILVER_SPEC, DQ_RULES_SPEC, DQ_ASSIGNMENT_SPEC, GOLD_SPEC)
}

# Enumerations validated before anything is written, so a typo in a YAML fails the CI
# job instead of the 3am ingestion run.
_ENUMS: Dict[Tuple[str, str], Sequence[str]] = {
    ("bronze_control_table", "load_type"): ("stream", "batch"),
    ("bronze_control_table", "source_file_type"): (
        "csv", "json", "parquet", "avro", "orc", "text", "binaryfile", "xml",
    ),
    ("bronze_control_table", "write_mode"): ("append", "merge", "overwrite"),
    ("bronze_control_table", "trigger_mode"): ("available_now", "processing_time", "continuous"),
    ("bronze_control_table", "schema_evolution_mode"): (
        "addnewcolumns", "rescue", "failonnewcolumns", "none",
    ),
    ("silver_control_table", "load_type"): ("append", "overwrite", "scd1", "scd2", "delete_insert"),
    ("silver_control_table", "read_mode"): ("batch", "stream", "cdf"),
    ("dq_rules", "rule_type"): ("sql", "function"),
    ("dq_rules", "default_severity"): ("drop", "warning", "fail"),
    ("dq_rules_assignment", "severity"): ("drop", "warning", "fail"),
    ("gold_control_table", "transformation_type"): ("module", "notebook", "sql"),
    ("gold_control_table", "load_type"): ("append", "overwrite", "scd1", "scd2", "delete_insert"),
    ("gold_control_table", "object_type"): ("dimension", "fact", "aggregate", "bridge"),
}


class MetadataLoader:
    """Loads metadata YAML files into the control tables as SCD Type-2."""

    def __init__(
        self,
        spark: SparkSession,
        cfg: FrameworkConfig,
        metadata_dir: Optional[str] = None,
        logger: Optional[FrameworkLogger] = None,
    ):
        self.spark = spark
        self.cfg = cfg
        self.metadata_dir = Path(metadata_dir) if metadata_dir else _default_metadata_dir()
        self.log = logger or FrameworkLogger({"component": "metadata_loader"}, cfg.log_level)

    # ---------------------------------------------------------------------------
    # public API
    # ---------------------------------------------------------------------------
    def load_all(self, prune_missing: bool = True, dry_run: bool = False) -> Dict[str, Dict[str, int]]:
        """Load every control table. Rule definitions load before their assignments."""
        order = [DQ_RULES_SPEC, BRONZE_SPEC, SILVER_SPEC, DQ_ASSIGNMENT_SPEC, GOLD_SPEC]
        results: Dict[str, Dict[str, int]] = {}
        for spec in order:
            results[spec.table_name] = self.load_table(spec, prune_missing=prune_missing, dry_run=dry_run)
        return results

    def load_table(
        self, spec: TableSpec, prune_missing: bool = True, dry_run: bool = False
    ) -> Dict[str, int]:
        """Load one control table from its YAML directory."""
        records, files = self._read_yaml_dir(spec)
        self.log.info(
            "metadata files parsed",
            table=spec.table_name,
            file_count=len(files),
            record_count=len(records),
        )
        if not records:
            return {"parsed": 0, "closed": 0, "inserted": 0, "pruned": 0}

        self._validate(spec, records)
        staged = self._stage(spec, records)

        if dry_run:
            drift = self._drift_count(spec, staged)
            self.log.info("dry run - no writes performed", table=spec.table_name, changed_rows=drift)
            return {"parsed": len(records), "closed": 0, "inserted": 0, "pruned": 0, "would_change": drift}

        staged.createOrReplaceTempView("_etl_staged")
        closed = self._close_changed(spec)
        inserted = self._insert_new(spec)
        pruned = self._prune(spec, files) if prune_missing else 0
        self.spark.catalog.dropTempView("_etl_staged")

        result = {"parsed": len(records), "closed": closed, "inserted": inserted, "pruned": pruned}
        self.log.info("control table loaded", table=spec.table_name, **result)
        return result

    # ---------------------------------------------------------------------------
    # YAML parsing
    # ---------------------------------------------------------------------------
    def _read_yaml_dir(self, spec: TableSpec) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Parse every *.yml / *.yaml under the spec's directory.

        A file may be a bare list, or a mapping with the spec's root key. An optional
        top-level `defaults:` block is merged underneath each entry, which removes the
        copy-paste that otherwise dominates these files.
        """
        directory = self.metadata_dir / spec.yaml_dir
        if not directory.exists():
            self.log.warning("metadata directory missing", table=spec.table_name, directory=str(directory))
            return [], []

        records: List[Dict[str, Any]] = []
        file_names: List[str] = []
        for path in sorted(list(directory.glob("*.yml")) + list(directory.glob("*.yaml"))):
            try:
                content = yaml.safe_load(path.read_text(encoding="utf-8"))
            except yaml.YAMLError as exc:
                raise MetadataValidationError(f"{path.name}: invalid YAML - {exc}") from exc
            if content is None:
                continue

            if isinstance(content, list):
                defaults, entries = {}, content
            elif isinstance(content, dict):
                defaults = content.get("defaults") or {}
                entries = content.get(spec.root_key)
                if entries is None:
                    raise MetadataValidationError(
                        f"{path.name}: expected a top-level '{spec.root_key}' list "
                        f"(found keys: {sorted(content)})"
                    )
            else:
                raise MetadataValidationError(f"{path.name}: expected a list or a mapping")

            if not isinstance(entries, list):
                raise MetadataValidationError(f"{path.name}: '{spec.root_key}' must be a list")

            file_names.append(path.name)
            for entry in entries:
                if not isinstance(entry, dict):
                    raise MetadataValidationError(f"{path.name}: every entry must be a mapping")
                merged = {**defaults, **entry}
                merged["config_file_name"] = path.name
                records.append(merged)
        return records, file_names

    # ---------------------------------------------------------------------------
    # validation
    # ---------------------------------------------------------------------------
    def _validate(self, spec: TableSpec, records: List[Dict[str, Any]]) -> None:
        known = set(spec.insert_columns)
        errors: List[str] = []
        seen: Dict[Tuple[str, ...], str] = {}

        for record in records:
            source = record.get("config_file_name", "<unknown>")
            label = f"{source}"

            unknown = sorted(set(record) - known)
            if unknown:
                errors.append(
                    f"{label}: unknown column(s) {unknown} for {spec.table_name}. "
                    f"Valid columns: {sorted(known)}"
                )

            for column in spec.required:
                value = record.get(column)
                if value is None or (isinstance(value, str) and not value.strip()) or value == []:
                    errors.append(f"{label}: '{column}' is required for {spec.table_name}")

            for column, allowed in ((c, a) for (t, c), a in _ENUMS.items() if t == spec.table_name):
                value = record.get(column)
                if value is not None and str(value).strip().lower() not in allowed:
                    errors.append(
                        f"{label}: {column}={value!r} is not one of {list(allowed)} for {spec.table_name}"
                    )

            key = self._business_key(spec, record)
            if all(part is not None for part in key):
                if key in seen:
                    errors.append(
                        f"{label}: duplicate business key {key} for {spec.table_name} "
                        f"(already defined in {seen[key]})"
                    )
                else:
                    seen[key] = label

        errors.extend(self._validate_cross_references(spec, records))

        if errors:
            raise MetadataValidationError(
                f"{len(errors)} metadata validation error(s) for {spec.table_name}:\n  - "
                + "\n  - ".join(errors)
            )

    def _validate_cross_references(self, spec: TableSpec, records: List[Dict[str, Any]]) -> List[str]:
        """Referential checks that span tables.

        A DQ assignment pointing at a rule_id that does not exist would silently drop
        the check at runtime, so it is rejected here instead.
        """
        errors: List[str] = []
        if spec is DQ_ASSIGNMENT_SPEC:
            registry_records, _ = self._read_yaml_dir(DQ_RULES_SPEC)
            yaml_rule_ids = {str(r.get("rule_id")) for r in registry_records}
            try:
                table_rule_ids = {
                    row["rule_id"]
                    for row in self.spark.sql(
                        f"SELECT rule_id FROM {self.cfg.control_table('dq_rules')} "
                        f"WHERE record_is_active = true"
                    ).collect()
                }
            except Exception:  # table not created yet on a first-ever deploy
                table_rule_ids = set()
            available = yaml_rule_ids | table_rule_ids
            for record in records:
                rule_id = str(record.get("rule_id"))
                if rule_id not in available:
                    errors.append(
                        f"{record.get('config_file_name')}: rule_id {rule_id!r} is not defined in "
                        f"conf/metadata/dq_rules nor present in dq_rules"
                    )
        if spec is GOLD_SPEC:
            for record in records:
                ttype = str(record.get("transformation_type", "")).lower()
                artefact_column = {
                    "module": "module_name",
                    "notebook": "notebook_name",
                    "sql": "sql_file_name",
                }.get(ttype)
                if artefact_column and not record.get(artefact_column):
                    errors.append(
                        f"{record.get('config_file_name')}: transformation_type={ttype} requires "
                        f"'{artefact_column}'"
                    )
        return errors

    @staticmethod
    def _business_key(spec: TableSpec, record: Dict[str, Any]) -> Tuple[Any, ...]:
        key: List[Any] = []
        for column in spec.business_keys:
            value = record.get(column)
            # silver_table_name defaults to source_table_name, so the key must too.
            if value is None and column == "silver_table_name":
                value = record.get("source_table_name")
            key.append(str(value).lower() if isinstance(value, str) else value)
        return tuple(key)

    # ---------------------------------------------------------------------------
    # staging
    # ---------------------------------------------------------------------------
    def _stage(self, spec: TableSpec, records: List[Dict[str, Any]]) -> DataFrame:
        """Build the staged DataFrame with the target's exact schema plus row_hash."""
        now = datetime.now(timezone.utc)
        created_by = self._current_user()

        rows: List[Dict[str, Any]] = []
        for record in records:
            normalised: Dict[str, Any] = {}
            for column, dtype in spec.columns:
                normalised[column] = _coerce(record.get(column), dtype)

            if spec is SILVER_SPEC and not normalised.get("silver_table_name"):
                normalised["silver_table_name"] = normalised.get("source_table_name")
            if normalised.get("is_enabled") is None:
                normalised["is_enabled"] = True

            payload = {c: normalised.get(c) for c in spec.attribute_columns if c != "config_file_name"}
            normalised["row_hash"] = sha256_of_payload(payload)
            normalised["record_start_ts"] = now
            normalised["record_end_ts"] = None
            normalised["record_is_active"] = True
            normalised["created_by"] = created_by
            rows.append(normalised)

        ordered = [tuple(r[name] for name, _ in spec.columns) for r in rows]
        return self.spark.createDataFrame(ordered, schema=spec.struct())

    def _current_user(self) -> str:
        try:
            return self.spark.sql("SELECT current_user() AS u").collect()[0]["u"]
        except Exception:  # pragma: no cover
            return "unknown"

    # ---------------------------------------------------------------------------
    # SCD2 passes
    # ---------------------------------------------------------------------------
    def _join_condition(self, spec: TableSpec) -> str:
        return " AND ".join(
            f"lower(COALESCE(t.`{c}`, '')) = lower(COALESCE(s.`{c}`, ''))"
            if dict(spec.columns)[c] == _STR
            else f"t.`{c}` = s.`{c}`"
            for c in spec.business_keys
        )

    def _close_changed(self, spec: TableSpec) -> int:
        target = self.cfg.control_table(spec.table_name)
        result = self.spark.sql(
            f"""
            MERGE INTO {target} t
            USING _etl_staged s
              ON {self._join_condition(spec)}
             AND t.record_is_active = true
            WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET
                 t.record_end_ts   = s.record_start_ts,
                 t.record_is_active = false
            """
        )
        return _merge_metric(result, "num_updated_rows")

    def _insert_new(self, spec: TableSpec) -> int:
        """Insert a fresh active version for every business key without one.

        Runs after _close_changed, so a changed key has no active row left and is
        inserted here, while an unchanged key still matches and is skipped. The
        identity column `id` is deliberately absent from the insert list.
        """
        target = self.cfg.control_table(spec.table_name)
        columns = spec.insert_columns
        insert_list = ", ".join(f"`{c}`" for c in columns)
        value_list = ", ".join(f"s.`{c}`" for c in columns)
        result = self.spark.sql(
            f"""
            MERGE INTO {target} t
            USING _etl_staged s
              ON {self._join_condition(spec)}
             AND t.record_is_active = true
            WHEN NOT MATCHED THEN INSERT ({insert_list}) VALUES ({value_list})
            """
        )
        return _merge_metric(result, "num_inserted_rows")

    def _prune(self, spec: TableSpec, file_names: List[str]) -> int:
        """End-date active rows whose business key is no longer in the YAML.

        Restricted to the config_file_name values seen in this run: loading only the
        crm YAML must never retire the erp feeds.
        """
        if not file_names:
            return 0
        target = self.cfg.control_table(spec.table_name)
        scope = ", ".join("'" + name.replace("'", "''") + "'" for name in file_names)
        result = self.spark.sql(
            f"""
            MERGE INTO {target} t
            USING _etl_staged s
              ON {self._join_condition(spec)}
             AND t.record_is_active = true
            WHEN NOT MATCHED BY SOURCE
             AND t.record_is_active = true
             AND t.config_file_name IN ({scope})
            THEN UPDATE SET
                 t.record_end_ts    = current_timestamp(),
                 t.record_is_active = false
            """
        )
        return _merge_metric(result, "num_updated_rows")

    def _drift_count(self, spec: TableSpec, staged: DataFrame) -> int:
        """How many staged rows would change the control table - used by dry_run."""
        target = self.cfg.control_table(spec.table_name)
        active = self.spark.sql(f"SELECT * FROM {target} WHERE record_is_active = true")
        keys = list(spec.business_keys)
        joined = staged.alias("s").join(
            active.select(*[F.col(k).alias(f"t_{k}") for k in keys], F.col("row_hash").alias("t_row_hash")),
            on=[F.col(f"s.{k}") == F.col(f"t_{k}") for k in keys],
            how="left",
        )
        return joined.filter(
            F.col("t_row_hash").isNull() | (F.col("t_row_hash") != F.col("s.row_hash"))
        ).count()


# =====================================================================================
# helpers
# =====================================================================================
def _default_metadata_dir() -> Path:
    return Path(__file__).resolve().parents[3] / "conf" / "metadata"


def _coerce(value: Any, dtype: Any) -> Any:
    """Coerce a YAML scalar into the control table's column type.

    YAML is permissive - `primary_keys: order_id` and `primary_keys: [order_id]` both
    read naturally - so single values are lifted into arrays and map values are
    stringified (MAP<STRING,STRING> cannot hold the booleans YAML produces for
    `header: true`).
    """
    if value is None:
        return None
    if isinstance(dtype, ArrayType):
        if isinstance(value, str):
            return [p.strip() for p in value.split(",") if p.strip()]
        if isinstance(value, (list, tuple)):
            return [str(v) for v in value]
        return [str(value)]
    if isinstance(dtype, MapType):
        if not isinstance(value, dict):
            raise MetadataValidationError(f"expected a mapping, received {type(value).__name__}: {value!r}")
        return {str(k): _yaml_scalar_to_str(v) for k, v in value.items()}
    if isinstance(dtype, BooleanType):
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"true", "1", "yes", "y"}
    if isinstance(dtype, IntegerType):
        return int(value)
    if isinstance(dtype, DoubleType):
        return float(value)
    if isinstance(dtype, TimestampType):
        return value
    return str(value)


def _yaml_scalar_to_str(value: Any) -> str:
    """Render a YAML scalar the way Spark options expect it (true, not True)."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _merge_metric(result: DataFrame, metric: str) -> int:
    """Read a metric out of the MERGE result frame, tolerating runtime differences."""
    try:
        row = result.collect()[0]
        return int(row[metric]) if metric in row.asDict() else 0
    except Exception:  # pragma: no cover - some runtimes return an empty frame
        return 0
