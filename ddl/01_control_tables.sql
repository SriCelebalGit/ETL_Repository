-- =====================================================================================
-- Metadata Driven ETL Framework :: CONTROL TABLES (DDL)  [FREE EDITION]
-- -------------------------------------------------------------------------------------
-- Deployed into the framework catalog/schema, e.g. workspace.etl_control
--
-- FREE EDITION: there is no CREATE CATALOG here. Free Edition gives you one workspace
-- catalog and creating others is not guaranteed, so the catalog named by
-- conf/framework.free.yml must already exist. Only the schema is created.
-- Executed by notebooks/00_setup_framework.py, which substitutes ${fw_catalog}
-- and ${fw_schema} from conf/framework.<env>.yml before running each statement.
--
-- Every control table is SCD Type-2: a logical row is identified by its BUSINESS KEY
-- (documented per table); superseded versions are closed with record_end_ts and
-- record_is_active = false.
--
-- Columns marked ENHANCEMENT are additions to the original design document.
-- =====================================================================================

CREATE SCHEMA IF NOT EXISTS ${fw_catalog}.${fw_schema};

-- =====================================================================================
-- BRONZE_CONTROL_TABLE
-- Business key: (target_catalog_name, bronze_schema_name, bronze_table_name)
-- Drives Auto Loader based landing -> bronze ingestion.
-- =====================================================================================
CREATE TABLE IF NOT EXISTS ${fw_catalog}.${fw_schema}.bronze_control_table (
    id                       BIGINT GENERATED ALWAYS AS IDENTITY,

    -- ---------------------------- source identification ----------------------------
    source_system            STRING    NOT NULL COMMENT 'Logical source system name, e.g. crm, erp, salesforce',
    source_entity_name       STRING             COMMENT 'ENHANCEMENT: object/entity name as known in the source system',
    source_file_type         STRING    NOT NULL COMMENT 'csv | json | parquet | avro | orc | text | binaryFile | xml',
    file_location            STRING    NOT NULL COMMENT 'Full landing path. FREE EDITION: a UC Volume path, e.g. /Volumes/workspace/etl_volumes/landing/crm/customer/',
    source_file_pattern      STRING             COMMENT 'ENHANCEMENT: cloudFiles.pathGlobFilter, e.g. *.csv',

    -- ---------------------------- target identification ----------------------------
    target_catalog_name      STRING    NOT NULL COMMENT 'ENHANCEMENT (missing in the original design): Unity Catalog catalog holding the bronze table',
    bronze_schema_name       STRING    NOT NULL COMMENT 'Bronze layer schema name',
    bronze_table_name        STRING    NOT NULL COMMENT 'Bronze layer table name',

    -- ---------------------------- Auto Loader behaviour ----------------------------
    load_type                STRING    NOT NULL COMMENT 'stream | batch (the "Type" column of the original design). Both use Auto Loader; batch means trigger(availableNow=True)',
    trigger_mode             STRING             COMMENT 'ENHANCEMENT: available_now | processing_time | continuous',
    trigger_interval         STRING             COMMENT 'ENHANCEMENT: e.g. "30 seconds", only with trigger_mode=processing_time',
    schema_location          STRING             COMMENT 'ENHANCEMENT: cloudFiles.schemaLocation. Derived from the framework checkpoint root when NULL',
    checkpoint_location      STRING             COMMENT 'ENHANCEMENT: structured streaming checkpoint. Derived when NULL',
    schema_evolution_mode    STRING             COMMENT 'ENHANCEMENT: addNewColumns (default) | rescue | failOnNewColumns | none',
    schema_hints             STRING             COMMENT 'ENHANCEMENT: cloudFiles.schemaHints, e.g. "order_id BIGINT, amount DECIMAL(18,2)"',
    rescued_data_column      STRING             COMMENT 'ENHANCEMENT: rescued data column name. Default _rescued_data',
    reader_options           MAP<STRING, STRING> COMMENT 'ENHANCEMENT: format reader options, e.g. {header: true, delimiter: "|", multiLine: true}',
    cloud_files_options      MAP<STRING, STRING> COMMENT 'ENHANCEMENT: raw cloudFiles.* overrides, e.g. {cloudFiles.useNotifications: true, cloudFiles.cleanSource: MOVE}',
    max_files_per_trigger    INT                COMMENT 'ENHANCEMENT: cloudFiles.maxFilesPerTrigger back-pressure',
    max_bytes_per_trigger    STRING             COMMENT 'ENHANCEMENT: cloudFiles.maxBytesPerTrigger, e.g. "10g"',

    -- ---------------------------- write behaviour ----------------------------
    write_mode               STRING             COMMENT 'ENHANCEMENT: append (default) | merge | overwrite',
    primary_keys             ARRAY<STRING>      COMMENT 'ENHANCEMENT: keys for write_mode=merge and for landing-level de-duplication',
    sequence_by              STRING             COMMENT 'ENHANCEMENT: ordering column used to pick the latest record per primary key',
    partition_columns        ARRAY<STRING>      COMMENT 'ENHANCEMENT: Delta partition columns',
    cluster_by               ARRAY<STRING>      COMMENT 'ENHANCEMENT: Delta liquid clustering columns (preferred over partitioning)',
    table_properties         MAP<STRING, STRING> COMMENT 'ENHANCEMENT: Delta table properties, e.g. {delta.enableChangeDataFeed: true}',
    add_ingestion_metadata   BOOLEAN            COMMENT 'ENHANCEMENT: add _ingest_ts / _source_file / _source_file_size / _batch_id columns. Default true',
    normalise_column_names   BOOLEAN            COMMENT 'ENHANCEMENT: normalise source column names to snake_case and strip Delta-illegal characters. Default true',

    -- ---------------------------- governance / lifecycle ----------------------------
    is_enabled               BOOLEAN            COMMENT 'ENHANCEMENT: disable a feed without deleting its metadata. Default true',
    config_file_name         STRING    NOT NULL COMMENT 'YAML file this row came from - traceability back to git',
    row_hash                 STRING             COMMENT 'ENHANCEMENT: SHA-256 of the attribute payload, used for SCD2 change detection',
    record_start_ts          TIMESTAMP NOT NULL COMMENT 'SCD2 version start',
    record_end_ts            TIMESTAMP          COMMENT 'SCD2 version end, NULL for the current version',
    record_is_active         BOOLEAN   NOT NULL COMMENT 'SCD2 current version flag',
    created_by               STRING             COMMENT 'ENHANCEMENT: principal that loaded this version'
)
USING DELTA
COMMENT 'Configuration driving Auto Loader ingestion of landing files into the bronze layer'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.columnMapping.mode = 'name');

-- =====================================================================================
-- SILVER_CONTROL_TABLE
-- Business key: (target_catalog_name, silver_schema_name, silver_table_name)
-- =====================================================================================
CREATE TABLE IF NOT EXISTS ${fw_catalog}.${fw_schema}.silver_control_table (
    id                       BIGINT GENERATED ALWAYS AS IDENTITY,

    -- ---------------------------- source (bronze) ----------------------------
    source_catalog_name      STRING    NOT NULL COMMENT 'ENHANCEMENT (missing in the original design): catalog of the source/bronze table',
    source_schema_name       STRING    NOT NULL COMMENT 'Source (bronze) schema name',
    source_table_name        STRING    NOT NULL COMMENT 'Source (bronze) table name (the "table_name" column of the original design)',

    -- ---------------------------- target (silver) ----------------------------
    target_catalog_name      STRING    NOT NULL COMMENT 'ENHANCEMENT (missing in the original design): catalog of the silver table',
    silver_schema_name       STRING    NOT NULL COMMENT 'Silver layer schema name',
    silver_table_name        STRING    NOT NULL COMMENT 'ENHANCEMENT: explicit silver table name, defaults to source_table_name',

    -- ---------------------------- load behaviour ----------------------------
    load_type                STRING    NOT NULL COMMENT 'append | overwrite | scd1 | scd2 | delete_insert',
    read_mode                STRING             COMMENT 'ENHANCEMENT: batch | stream | cdf. stream/cdf give incremental bronze -> silver reads',
    business_keys            ARRAY<STRING> NOT NULL COMMENT 'ENHANCEMENT: natural/business key columns for SCD1/SCD2 merges',
    sequence_by              STRING             COMMENT 'ENHANCEMENT: ordering column resolving multiple changes per key inside one batch',
    scd2_hash_columns        ARRAY<STRING>      COMMENT 'ENHANCEMENT: columns compared for SCD2 change detection. NULL means all non-key non-audit columns',
    watermark_column         STRING             COMMENT 'ENHANCEMENT: high-water-mark column for read_mode=batch incremental pulls',
    select_expressions       ARRAY<STRING>      COMMENT 'ENHANCEMENT: projection / rename / cast list, e.g. ["cust_id AS customer_id", "upper(name) AS customer_name"]',
    filter_condition         STRING             COMMENT 'ENHANCEMENT: SQL predicate applied to the source read',
    drop_columns             ARRAY<STRING>      COMMENT 'ENHANCEMENT: columns removed before the silver write',
    deduplicate              BOOLEAN            COMMENT 'ENHANCEMENT: de-duplicate on business_keys + sequence_by before loading. Default true',
    partition_columns        ARRAY<STRING>      COMMENT 'ENHANCEMENT: Delta partition columns',
    cluster_by               ARRAY<STRING>      COMMENT 'ENHANCEMENT: liquid clustering columns',
    table_properties         MAP<STRING, STRING> COMMENT 'ENHANCEMENT: Delta table properties',
    checkpoint_location      STRING             COMMENT 'ENHANCEMENT: checkpoint for read_mode=stream/cdf. Derived when NULL',

    -- ---------------------------- data quality ----------------------------
    dq_enabled               BOOLEAN            COMMENT 'ENHANCEMENT: run the DQ engine for this table. Default true',
    quarantine_enabled       BOOLEAN            COMMENT 'ENHANCEMENT: persist failing rows. Default true',
    quarantine_table_name    STRING             COMMENT 'ENHANCEMENT: defaults to <silver_table_name>_quarantine',
    dq_failure_threshold_pct DOUBLE             COMMENT 'ENHANCEMENT: abort the task when the quarantined share of the batch exceeds this percentage. NULL disables the check',

    -- ---------------------------- governance / lifecycle ----------------------------
    is_enabled               BOOLEAN            COMMENT 'ENHANCEMENT: disable without deleting metadata. Default true',
    config_file_name         STRING    NOT NULL,
    row_hash                 STRING,
    record_start_ts          TIMESTAMP NOT NULL,
    record_end_ts            TIMESTAMP,
    record_is_active         BOOLEAN   NOT NULL,
    created_by               STRING
)
USING DELTA
COMMENT 'Configuration driving bronze -> silver curation (DQ checks plus SCD1/SCD2 loading)'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.columnMapping.mode = 'name');

-- =====================================================================================
-- DQ_RULES  (rule registry)
-- Business key: (rule_id)
-- =====================================================================================
CREATE TABLE IF NOT EXISTS ${fw_catalog}.${fw_schema}.dq_rules (
    id                       BIGINT GENERATED ALWAYS AS IDENTITY,
    rule_id                  STRING    NOT NULL COMMENT 'Unique id explicitly declared in the YAML when a new rule is created',
    rule_name                STRING             COMMENT 'ENHANCEMENT: short human readable name',
    rule_type                STRING    NOT NULL COMMENT 'sql | function. sql = a boolean SQL clause, function = a registered python callable',
    rule                     STRING    NOT NULL COMMENT 'For sql: the whole boolean expression, TRUE means the row PASSES. For function: the registered function name',
    rule_parameters          MAP<STRING, STRING> COMMENT 'ENHANCEMENT: default parameters, e.g. {min: 0, max: 100} or {pattern: "^[A-Z]{2}$"}. {column} and {param} placeholders in `rule` are rendered at runtime',
    dq_dimension             STRING             COMMENT 'ENHANCEMENT: completeness | validity | uniqueness | consistency | accuracy | timeliness',
    default_severity         STRING             COMMENT 'ENHANCEMENT: severity used when an assignment does not override it',
    description              STRING             COMMENT 'Rule description',
    is_enabled               BOOLEAN            COMMENT 'ENHANCEMENT: retire a rule without deleting it. Default true',
    config_file_name         STRING    NOT NULL,
    row_hash                 STRING,
    record_start_ts          TIMESTAMP NOT NULL,
    record_end_ts            TIMESTAMP,
    record_is_active         BOOLEAN   NOT NULL,
    created_by               STRING
)
USING DELTA
COMMENT 'Registry of reusable data quality rules'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.columnMapping.mode = 'name');

-- =====================================================================================
-- DQ_RULES_ASSIGNMENT
-- Business key: (catalog_name, schema_name, table_name, column_name, rule_id)
-- =====================================================================================
CREATE TABLE IF NOT EXISTS ${fw_catalog}.${fw_schema}.dq_rules_assignment (
    id                       BIGINT GENERATED ALWAYS AS IDENTITY,
    catalog_name             STRING    NOT NULL COMMENT 'ENHANCEMENT (missing in the original design): catalog of the table being checked',
    schema_name              STRING    NOT NULL COMMENT 'ENHANCEMENT (missing in the original design): schema of the table being checked',
    table_name               STRING    NOT NULL COMMENT 'Table whose column is being DQ checked',
    column_name              STRING    NOT NULL COMMENT 'Column being checked. Use __table__ for row/table level rules',
    rule_id                  STRING    NOT NULL COMMENT 'rule_id from dq_rules',
    severity                 STRING    NOT NULL COMMENT 'drop | warning | fail. drop = quarantine and exclude the row, warning = quarantine a copy and keep the row, fail = abort the task (ENHANCEMENT)',
    rule_parameters          MAP<STRING, STRING> COMMENT 'ENHANCEMENT: per-assignment parameter overrides',
    filter_condition         STRING             COMMENT 'ENHANCEMENT: apply the rule only to rows matching this predicate',
    is_enabled               BOOLEAN            COMMENT 'ENHANCEMENT: switch a single check off. Default true',
    config_file_name         STRING    NOT NULL,
    row_hash                 STRING,
    record_start_ts          TIMESTAMP NOT NULL,
    record_end_ts            TIMESTAMP,
    record_is_active         BOOLEAN   NOT NULL,
    created_by               STRING
)
USING DELTA
COMMENT 'Assignment of DQ rules to specific table columns, with a severity'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.columnMapping.mode = 'name');

-- =====================================================================================
-- GOLD_CONTROL_TABLE
-- Business key: (target_catalog_name, target_schema_name, table_name)
-- =====================================================================================
CREATE TABLE IF NOT EXISTS ${fw_catalog}.${fw_schema}.gold_control_table (
    id                       BIGINT GENERATED ALWAYS AS IDENTITY,
    target_catalog_name      STRING    NOT NULL COMMENT 'ENHANCEMENT (missing in the original design): catalog of the gold table',
    target_schema_name       STRING    NOT NULL COMMENT 'Target schema name (the "schema_name" column of the original design)',
    table_name               STRING    NOT NULL COMMENT 'Target table name produced by the transformation',
    object_type              STRING             COMMENT 'ENHANCEMENT: dimension | fact | aggregate | bridge',
    transformation_type      STRING    NOT NULL COMMENT 'ENHANCEMENT: module | notebook | sql. module = importable python module, unit-testable and preferred',
    notebook_name            STRING             COMMENT 'Transformation notebook path (transformation_type=notebook)',
    module_name              STRING             COMMENT 'ENHANCEMENT: dotted python module exposing transform(spark, ctx) -> DataFrame',
    sql_file_name            STRING             COMMENT 'ENHANCEMENT: .sql file whose single SELECT produces the target (transformation_type=sql)',
    load_type                STRING    NOT NULL COMMENT 'ENHANCEMENT: overwrite | append | scd1 | scd2 | delete_insert',
    business_keys            ARRAY<STRING>      COMMENT 'ENHANCEMENT: natural keys for merge based loads',
    sequence_by              STRING             COMMENT 'ENHANCEMENT: ordering column for merge based loads',
    scd2_hash_columns        ARRAY<STRING>      COMMENT 'ENHANCEMENT: columns compared for SCD2 change detection',
    depends_on               ARRAY<STRING>      COMMENT 'ENHANCEMENT: fully qualified gold tables that must load first - used to generate job task dependencies',
    parameters               MAP<STRING, STRING> COMMENT 'ENHANCEMENT: parameters handed to the transformation, e.g. {lookback_days: 7}',
    partition_columns        ARRAY<STRING>      COMMENT 'ENHANCEMENT: Delta partition columns',
    cluster_by               ARRAY<STRING>      COMMENT 'ENHANCEMENT: liquid clustering columns',
    table_properties         MAP<STRING, STRING> COMMENT 'ENHANCEMENT: Delta table properties',
    is_enabled               BOOLEAN            COMMENT 'ENHANCEMENT: default true',
    config_file_name         STRING    NOT NULL,
    row_hash                 STRING,
    record_start_ts          TIMESTAMP NOT NULL COMMENT 'record_start_timestamp in the original design, renamed for consistency with the other control tables',
    record_end_ts            TIMESTAMP,
    record_is_active         BOOLEAN   NOT NULL,
    created_by               STRING
)
USING DELTA
COMMENT 'Configuration driving silver -> gold dimensional modelling'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.columnMapping.mode = 'name');
