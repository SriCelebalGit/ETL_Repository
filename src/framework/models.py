"""Typed views over the control table rows.

The runners work against these dataclasses rather than raw Rows so that a missing or
misspelled control column fails once, at construction, with a clear message - instead
of surfacing 300 lines later as an AttributeError inside a foreachBatch.

Each `from_row` applies the framework defaults for the columns the design leaves
optional, so the rest of the code never has to write `or "append"` again.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pyspark.sql import Row

from .config import FrameworkConfig
from .exceptions import ControlTableError

# Framework audit columns added to bronze rows. Excluded from SCD2 change detection
# (see transform.scd) so a fresh _ingest_ts never looks like a business change.
INGESTION_METADATA_COLUMNS = (
    "_ingest_ts",
    "_source_file",
    "_source_file_size",
    "_source_file_mod_time",
    "_batch_id",
)


def _as_dict(row: Any) -> Dict[str, Any]:
    if isinstance(row, Row):
        return row.asDict(recursive=True)
    if isinstance(row, dict):
        return dict(row)
    raise ControlTableError(f"Expected a Row or dict, received {type(row)!r}")


def _require(data: Dict[str, Any], key: str, table: str) -> Any:
    value = data.get(key)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ControlTableError(f"{table}: required column '{key}' is null or empty (row id={data.get('id')})")
    return value


def _list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [p.strip() for p in value.split(",") if p.strip()]
    return [str(v).strip() for v in value if str(v).strip()]


def _map(value: Any) -> Dict[str, str]:
    if not value:
        return {}
    return {str(k): str(v) for k, v in dict(value).items()}


def _bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


# =====================================================================================
# Bronze
# =====================================================================================
@dataclass
class BronzeConfig:
    """One row of bronze_control_table, resolved and defaulted."""

    control_row_id: Optional[int]
    source_system: str
    source_entity_name: Optional[str]
    source_file_type: str
    file_location: str
    source_file_pattern: Optional[str]

    catalog: str
    schema: str
    table: str

    load_type: str
    trigger_mode: str
    trigger_interval: Optional[str]
    schema_location: str
    checkpoint_location: str
    schema_evolution_mode: str
    schema_hints: Optional[str]
    rescued_data_column: str
    reader_options: Dict[str, str] = field(default_factory=dict)
    cloud_files_options: Dict[str, str] = field(default_factory=dict)
    max_files_per_trigger: Optional[int] = None
    max_bytes_per_trigger: Optional[str] = None

    write_mode: str = "append"
    primary_keys: List[str] = field(default_factory=list)
    sequence_by: Optional[str] = None
    partition_columns: List[str] = field(default_factory=list)
    cluster_by: List[str] = field(default_factory=list)
    table_properties: Dict[str, str] = field(default_factory=dict)
    add_ingestion_metadata: bool = True
    normalise_column_names: bool = True

    config_file_name: Optional[str] = None

    @property
    def full_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"

    @classmethod
    def from_row(cls, row: Any, cfg: FrameworkConfig) -> "BronzeConfig":
        data = _as_dict(row)
        t = "bronze_control_table"

        catalog = cfg.resolve_catalog(_require(data, "target_catalog_name", t))
        schema = _require(data, "bronze_schema_name", t)
        table = _require(data, "bronze_table_name", t)

        load_type = str(_require(data, "load_type", t)).strip().lower()
        if load_type not in {"stream", "batch"}:
            raise ControlTableError(f"{t}: load_type must be 'stream' or 'batch', got {load_type!r}")

        # batch feeds default to availableNow, streaming feeds to a fixed micro-batch
        # interval - the mode the design's "Stream/Batch" column implies.
        default_trigger = "available_now" if load_type == "batch" else "processing_time"
        trigger_mode = str(data.get("trigger_mode") or default_trigger).strip().lower()

        write_mode = str(data.get("write_mode") or cfg.default_for("bronze_write_mode", "append")).lower()
        primary_keys = _list(data.get("primary_keys"))
        if write_mode == "merge" and not primary_keys:
            raise ControlTableError(f"{t}: write_mode='merge' requires primary_keys ({catalog}.{schema}.{table})")

        return cls(
            control_row_id=data.get("id"),
            source_system=_require(data, "source_system", t),
            source_entity_name=data.get("source_entity_name"),
            source_file_type=str(_require(data, "source_file_type", t)).strip().lower(),
            file_location=_require(data, "file_location", t),
            source_file_pattern=data.get("source_file_pattern"),
            catalog=catalog,
            schema=schema,
            table=table,
            load_type=load_type,
            trigger_mode=trigger_mode,
            trigger_interval=data.get("trigger_interval") or cfg.default_for("trigger_interval", "1 minute"),
            schema_location=data.get("schema_location")
            or cfg.checkpoint_path("bronze", catalog, schema, table, "schema"),
            checkpoint_location=data.get("checkpoint_location")
            or cfg.checkpoint_path("bronze", catalog, schema, table, "checkpoint"),
            schema_evolution_mode=data.get("schema_evolution_mode")
            or cfg.default_for("schema_evolution_mode", "addNewColumns"),
            schema_hints=data.get("schema_hints"),
            rescued_data_column=data.get("rescued_data_column")
            or cfg.default_for("rescued_data_column", "_rescued_data"),
            reader_options=_map(data.get("reader_options")),
            cloud_files_options=_map(data.get("cloud_files_options")),
            max_files_per_trigger=data.get("max_files_per_trigger")
            or cfg.default_for("max_files_per_trigger"),
            max_bytes_per_trigger=data.get("max_bytes_per_trigger")
            or cfg.default_for("max_bytes_per_trigger"),
            write_mode=write_mode,
            primary_keys=primary_keys,
            sequence_by=data.get("sequence_by"),
            partition_columns=_list(data.get("partition_columns")),
            cluster_by=_list(data.get("cluster_by")),
            table_properties=_map(data.get("table_properties")),
            add_ingestion_metadata=_bool(data.get("add_ingestion_metadata"), True),
            normalise_column_names=_bool(data.get("normalise_column_names"), True),
            config_file_name=data.get("config_file_name"),
        )


# =====================================================================================
# Silver
# =====================================================================================
@dataclass
class SilverConfig:
    """One row of silver_control_table, resolved and defaulted."""

    control_row_id: Optional[int]
    source_catalog: str
    source_schema: str
    source_table: str
    target_catalog: str
    target_schema: str
    target_table: str

    load_type: str
    read_mode: str
    business_keys: List[str]
    sequence_by: Optional[str]
    scd2_hash_columns: List[str] = field(default_factory=list)
    watermark_column: Optional[str] = None
    select_expressions: List[str] = field(default_factory=list)
    filter_condition: Optional[str] = None
    drop_columns: List[str] = field(default_factory=list)
    deduplicate: bool = True
    partition_columns: List[str] = field(default_factory=list)
    cluster_by: List[str] = field(default_factory=list)
    table_properties: Dict[str, str] = field(default_factory=dict)
    checkpoint_location: Optional[str] = None

    dq_enabled: bool = True
    quarantine_enabled: bool = True
    quarantine_table: str = ""
    dq_failure_threshold_pct: Optional[float] = None

    config_file_name: Optional[str] = None

    LOAD_TYPES = {"append", "overwrite", "scd1", "scd2", "delete_insert"}
    READ_MODES = {"batch", "stream", "cdf"}

    @property
    def source_full_name(self) -> str:
        return f"{self.source_catalog}.{self.source_schema}.{self.source_table}"

    @property
    def target_full_name(self) -> str:
        return f"{self.target_catalog}.{self.target_schema}.{self.target_table}"

    @classmethod
    def from_row(cls, row: Any, cfg: FrameworkConfig) -> "SilverConfig":
        data = _as_dict(row)
        t = "silver_control_table"

        source_catalog = cfg.resolve_catalog(_require(data, "source_catalog_name", t))
        source_schema = _require(data, "source_schema_name", t)
        source_table = _require(data, "source_table_name", t)
        target_catalog = cfg.resolve_catalog(_require(data, "target_catalog_name", t))
        target_schema = _require(data, "silver_schema_name", t)
        target_table = data.get("silver_table_name") or source_table

        load_type = str(_require(data, "load_type", t)).strip().lower()
        if load_type not in cls.LOAD_TYPES:
            raise ControlTableError(f"{t}: load_type {load_type!r} not in {sorted(cls.LOAD_TYPES)}")

        read_mode = str(data.get("read_mode") or cfg.default_for("silver_read_mode", "batch")).lower()
        if read_mode not in cls.READ_MODES:
            raise ControlTableError(f"{t}: read_mode {read_mode!r} not in {sorted(cls.READ_MODES)}")

        business_keys = _list(data.get("business_keys"))
        if load_type in {"scd1", "scd2", "delete_insert"} and not business_keys:
            raise ControlTableError(
                f"{t}: load_type={load_type} requires business_keys "
                f"({target_catalog}.{target_schema}.{target_table})"
            )

        quarantine_table = data.get("quarantine_table_name") or f"{target_table}_quarantine"

        return cls(
            control_row_id=data.get("id"),
            source_catalog=source_catalog,
            source_schema=source_schema,
            source_table=source_table,
            target_catalog=target_catalog,
            target_schema=target_schema,
            target_table=target_table,
            load_type=load_type,
            read_mode=read_mode,
            business_keys=business_keys,
            sequence_by=data.get("sequence_by"),
            scd2_hash_columns=_list(data.get("scd2_hash_columns")),
            watermark_column=data.get("watermark_column"),
            select_expressions=_list(data.get("select_expressions")),
            filter_condition=data.get("filter_condition"),
            drop_columns=_list(data.get("drop_columns")),
            deduplicate=_bool(data.get("deduplicate"), True),
            partition_columns=_list(data.get("partition_columns")),
            cluster_by=_list(data.get("cluster_by")),
            table_properties=_map(data.get("table_properties")),
            checkpoint_location=data.get("checkpoint_location")
            or cfg.checkpoint_path("silver", target_catalog, target_schema, target_table, "checkpoint"),
            dq_enabled=_bool(data.get("dq_enabled"), True),
            quarantine_enabled=_bool(data.get("quarantine_enabled"), True),
            quarantine_table=quarantine_table,
            dq_failure_threshold_pct=data.get("dq_failure_threshold_pct"),
            config_file_name=data.get("config_file_name"),
        )


# =====================================================================================
# Gold
# =====================================================================================
@dataclass
class GoldConfig:
    """One row of gold_control_table, resolved and defaulted."""

    control_row_id: Optional[int]
    target_catalog: str
    target_schema: str
    table: str
    object_type: Optional[str]
    transformation_type: str
    notebook_name: Optional[str]
    module_name: Optional[str]
    sql_file_name: Optional[str]
    load_type: str
    business_keys: List[str] = field(default_factory=list)
    sequence_by: Optional[str] = None
    scd2_hash_columns: List[str] = field(default_factory=list)
    depends_on: List[str] = field(default_factory=list)
    parameters: Dict[str, str] = field(default_factory=dict)
    partition_columns: List[str] = field(default_factory=list)
    cluster_by: List[str] = field(default_factory=list)
    table_properties: Dict[str, str] = field(default_factory=dict)
    config_file_name: Optional[str] = None

    TRANSFORMATION_TYPES = {"module", "notebook", "sql"}
    LOAD_TYPES = {"append", "overwrite", "scd1", "scd2", "delete_insert"}

    @property
    def full_name(self) -> str:
        return f"{self.target_catalog}.{self.target_schema}.{self.table}"

    @classmethod
    def from_row(cls, row: Any, cfg: FrameworkConfig) -> "GoldConfig":
        data = _as_dict(row)
        t = "gold_control_table"

        target_catalog = cfg.resolve_catalog(_require(data, "target_catalog_name", t))
        target_schema = _require(data, "target_schema_name", t)
        table = _require(data, "table_name", t)

        transformation_type = str(_require(data, "transformation_type", t)).strip().lower()
        if transformation_type not in cls.TRANSFORMATION_TYPES:
            raise ControlTableError(
                f"{t}: transformation_type {transformation_type!r} not in {sorted(cls.TRANSFORMATION_TYPES)}"
            )
        artefact_column = {
            "module": "module_name",
            "notebook": "notebook_name",
            "sql": "sql_file_name",
        }[transformation_type]
        if not data.get(artefact_column):
            raise ControlTableError(
                f"{t}: transformation_type={transformation_type} requires '{artefact_column}' "
                f"to be populated ({target_catalog}.{target_schema}.{table})"
            )

        load_type = str(_require(data, "load_type", t)).strip().lower()
        if load_type not in cls.LOAD_TYPES:
            raise ControlTableError(f"{t}: load_type {load_type!r} not in {sorted(cls.LOAD_TYPES)}")

        business_keys = _list(data.get("business_keys"))
        if load_type in {"scd1", "scd2", "delete_insert"} and not business_keys:
            raise ControlTableError(f"{t}: load_type={load_type} requires business_keys ({table})")

        return cls(
            control_row_id=data.get("id"),
            target_catalog=target_catalog,
            target_schema=target_schema,
            table=table,
            object_type=data.get("object_type"),
            transformation_type=transformation_type,
            notebook_name=data.get("notebook_name"),
            module_name=data.get("module_name"),
            sql_file_name=data.get("sql_file_name"),
            load_type=load_type,
            business_keys=business_keys,
            sequence_by=data.get("sequence_by"),
            scd2_hash_columns=_list(data.get("scd2_hash_columns")),
            depends_on=_list(data.get("depends_on")),
            parameters=_map(data.get("parameters")),
            partition_columns=_list(data.get("partition_columns")),
            cluster_by=_list(data.get("cluster_by")),
            table_properties=_map(data.get("table_properties")),
            config_file_name=data.get("config_file_name"),
        )


# =====================================================================================
# DQ
# =====================================================================================
@dataclass
class DQRule:
    """A dq_rules row joined to one dq_rules_assignment row."""

    rule_id: str
    rule_type: str
    rule: str
    column_name: str
    severity: str
    parameters: Dict[str, str] = field(default_factory=dict)
    filter_condition: Optional[str] = None
    dq_dimension: Optional[str] = None
    rule_name: Optional[str] = None
    description: Optional[str] = None

    SEVERITIES = {"drop", "warning", "fail"}
    RULE_TYPES = {"sql", "function"}

    @property
    def is_table_level(self) -> bool:
        """__table__ marks a rule that spans the row rather than a single column."""
        return self.column_name == "__table__"

    @classmethod
    def from_row(cls, row: Any) -> "DQRule":
        data = _as_dict(row)
        rule_id = _require(data, "rule_id", "dq_rules")
        rule_type = str(_require(data, "rule_type", "dq_rules")).strip().lower()
        if rule_type not in cls.RULE_TYPES:
            raise ControlTableError(f"dq_rules: rule_type {rule_type!r} not in {sorted(cls.RULE_TYPES)}")

        severity = str(data.get("severity") or data.get("default_severity") or "drop").strip().lower()
        if severity not in cls.SEVERITIES:
            raise ControlTableError(
                f"dq_rules_assignment: severity {severity!r} for rule {rule_id} not in {sorted(cls.SEVERITIES)}"
            )

        # Assignment level parameters win over the registry defaults.
        parameters = _map(data.get("rule_parameters_default"))
        parameters.update(_map(data.get("rule_parameters")))

        return cls(
            rule_id=rule_id,
            rule_type=rule_type,
            rule=_require(data, "rule", "dq_rules"),
            column_name=_require(data, "column_name", "dq_rules_assignment"),
            severity=severity,
            parameters=parameters,
            filter_condition=data.get("filter_condition"),
            dq_dimension=data.get("dq_dimension"),
            rule_name=data.get("rule_name"),
            description=data.get("description"),
        )
