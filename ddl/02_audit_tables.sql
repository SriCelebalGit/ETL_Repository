-- =====================================================================================
-- Metadata Driven ETL Framework :: AUDIT TABLES (DDL)
-- -------------------------------------------------------------------------------------
-- Deployed into the framework catalog/schema, e.g. workspace.etl_audit
-- ${fw_catalog} / ${fw_audit_schema} are substituted by notebooks/00_setup_framework.py
--
-- batch_id threads every row across every audit table: it is the master workflow job
-- run id, so one Lakehouse batch can be reconstructed end to end.
-- =====================================================================================

CREATE SCHEMA IF NOT EXISTS ${fw_catalog}.${fw_audit_schema};

-- =====================================================================================
-- JOB_RUN_AUDIT
-- One row per (job run, task) across every layer. Written twice: RUNNING at task start,
-- then updated to SUCCEEDED / FAILED / SKIPPED on completion.
-- =====================================================================================
CREATE TABLE IF NOT EXISTS ${fw_catalog}.${fw_audit_schema}.job_run_audit (
    audit_id            STRING    NOT NULL COMMENT 'ENHANCEMENT: uuid of this audit row, used to update it on task completion',
    batch_id            STRING    NOT NULL COMMENT 'Master workflow job run id - the unique batch identifier',
    job_id              STRING             COMMENT 'Job id of the workflow job',
    job_name            STRING             COMMENT 'Name of the job',
    job_run_id          STRING             COMMENT 'Workflow job run id',
    task_name           STRING             COMMENT 'ENHANCEMENT: workflow task key - gives task level granularity',
    task_run_id         STRING             COMMENT 'ENHANCEMENT: workflow task run id',
    layer               STRING    NOT NULL COMMENT 'bronze | silver | gold | control | audit',
    catalog_name        STRING             COMMENT 'ENHANCEMENT: target catalog',
    schema_name         STRING             COMMENT 'ENHANCEMENT: target schema',
    table_name          STRING             COMMENT 'ENHANCEMENT: target table',
    source_object       STRING             COMMENT 'ENHANCEMENT: source path or fully qualified source table',
    job_status          STRING    NOT NULL COMMENT 'RUNNING | SUCCEEDED | FAILED | SKIPPED',
    records_read        BIGINT             COMMENT 'ENHANCEMENT: rows read from the source',
    records_inserted    BIGINT             COMMENT 'ENHANCEMENT: rows inserted into the target',
    records_updated     BIGINT             COMMENT 'ENHANCEMENT: rows updated in the target',
    records_deleted     BIGINT             COMMENT 'ENHANCEMENT: rows deleted / SCD2 closed',
    records_rejected    BIGINT             COMMENT 'ENHANCEMENT: rows quarantined by DQ',
    files_processed     BIGINT             COMMENT 'ENHANCEMENT: Auto Loader files consumed in this run',
    bytes_processed     BIGINT             COMMENT 'ENHANCEMENT: Auto Loader bytes consumed in this run',
    target_row_count    BIGINT             COMMENT 'ENHANCEMENT: target table row count after the load',
    error_message       STRING             COMMENT 'ENHANCEMENT: exception message when job_status=FAILED',
    error_stacktrace    STRING             COMMENT 'ENHANCEMENT: truncated stack trace for debugging',
    retry_count         INT                COMMENT 'ENHANCEMENT: attempt number within the run',
    task_start_timestamp TIMESTAMP NOT NULL COMMENT 'Task start timestamp',
    task_end_timestamp   TIMESTAMP         COMMENT 'Task end timestamp',
    duration_seconds     DOUBLE            COMMENT 'ENHANCEMENT: task duration',
    environment          STRING            COMMENT 'ENHANCEMENT: dev | tst | prd',
    framework_version    STRING            COMMENT 'ENHANCEMENT: framework release that produced this row',
    run_by               STRING            COMMENT 'ENHANCEMENT: principal the task ran as',
    cluster_id           STRING            COMMENT 'ENHANCEMENT: compute the task ran on',
    control_row_id       BIGINT            COMMENT 'ENHANCEMENT: id of the control table row used - ties the run back to the exact config version',
    audit_insert_ts      TIMESTAMP NOT NULL COMMENT 'ENHANCEMENT: when this audit row was written'
)
USING DELTA
PARTITIONED BY (layer)
COMMENT 'Job/task level run audit across every ETL layer'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.columnMapping.mode = 'name');

-- =====================================================================================
-- DQ_RUN_AUDIT
-- One row per (batch, silver table) summarising the DQ outcome for that batch.
-- =====================================================================================
CREATE TABLE IF NOT EXISTS ${fw_catalog}.${fw_audit_schema}.dq_run_audit (
    audit_id                STRING    NOT NULL COMMENT 'ENHANCEMENT: uuid of this audit row',
    batch_id                STRING    NOT NULL COMMENT 'Master workflow job run id - the unique batch identifier',
    dq_task_run_id          STRING             COMMENT 'DQ pipeline run id',
    source_catalog_name     STRING             COMMENT 'ENHANCEMENT: catalog of the table under DQ check',
    source_schema_name      STRING             COMMENT 'Schema of the table under DQ check',
    table_name              STRING    NOT NULL COMMENT 'Table under DQ check',
    target_catalog_name     STRING             COMMENT 'ENHANCEMENT: target catalog',
    target_schema_name      STRING             COMMENT 'Target table schema name',
    target_table_name       STRING             COMMENT 'ENHANCEMENT: target table name',
    quarantine_table_name   STRING             COMMENT 'ENHANCEMENT: fully qualified quarantine table',
    pipeline_status         STRING    NOT NULL COMMENT 'Data quality pipeline status: SUCCEEDED | FAILED',
    dq_check_outcome        STRING    NOT NULL COMMENT 'PASSED | QUARANTINED | THRESHOLD_BREACHED | NO_RULES',
    rules_evaluated         INT                COMMENT 'ENHANCEMENT: number of rule assignments evaluated',
    rules_failed            INT                COMMENT 'ENHANCEMENT: number of rule assignments with at least one failing row',
    src_rec_count           BIGINT             COMMENT 'Source table record count',
    quarantine_count        BIGINT             COMMENT 'Quarantined record count',
    warning_count           BIGINT             COMMENT 'ENHANCEMENT: rows that failed a warning-severity rule but were still loaded',
    target_rec_count        BIGINT             COMMENT 'Target table record count',
    quarantine_pct          DOUBLE             COMMENT 'ENHANCEMENT: quarantine_count / src_rec_count * 100',
    error_message           STRING             COMMENT 'ENHANCEMENT: failure detail',
    dq_task_start_timestamp TIMESTAMP NOT NULL COMMENT 'Data quality check start timestamp',
    dq_task_end_timestamp   TIMESTAMP          COMMENT 'Data quality check end timestamp',
    environment             STRING             COMMENT 'ENHANCEMENT: dev | tst | prd',
    audit_insert_ts         TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Per batch, per table data quality run summary for the silver layer'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.columnMapping.mode = 'name');

-- =====================================================================================
-- DQ_RESULT_DETAIL   (ENHANCEMENT)
-- One row per (batch, table, column, rule). This is what makes a DQ trend dashboard
-- possible - the summary table alone cannot answer "which rule is degrading".
-- =====================================================================================
CREATE TABLE IF NOT EXISTS ${fw_catalog}.${fw_audit_schema}.dq_result_detail (
    audit_id            STRING    NOT NULL,
    batch_id            STRING    NOT NULL,
    dq_task_run_id      STRING,
    catalog_name        STRING,
    schema_name         STRING,
    table_name          STRING    NOT NULL,
    column_name         STRING    NOT NULL,
    rule_id             STRING    NOT NULL,
    rule_type           STRING,
    rule_expression     STRING             COMMENT 'The rendered expression actually evaluated - critical when rules are parameterised',
    dq_dimension        STRING,
    severity            STRING    NOT NULL,
    rows_evaluated      BIGINT,
    rows_failed         BIGINT,
    pass_pct            DOUBLE,
    rule_status         STRING    NOT NULL COMMENT 'PASSED | FAILED | ERRORED',
    error_message       STRING,
    evaluated_at        TIMESTAMP NOT NULL,
    environment         STRING,
    audit_insert_ts     TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'ENHANCEMENT: per rule DQ results, enabling rule level trend analysis'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.columnMapping.mode = 'name');

-- =====================================================================================
-- BRONZE_FILE_AUDIT   (ENHANCEMENT)
-- File level lineage for Auto Loader ingestion. Answers "which file did this row
-- come from, and when was that file picked up".
-- =====================================================================================
CREATE TABLE IF NOT EXISTS ${fw_catalog}.${fw_audit_schema}.bronze_file_audit (
    batch_id                STRING    NOT NULL,
    stream_batch_id         BIGINT             COMMENT 'Structured streaming micro-batch id',
    catalog_name            STRING,
    schema_name             STRING,
    table_name              STRING    NOT NULL,
    source_file_path        STRING    NOT NULL,
    source_file_name        STRING,
    source_file_size        BIGINT,
    source_file_mod_time    TIMESTAMP,
    record_count            BIGINT,
    rescued_record_count    BIGINT             COMMENT 'Rows in this file that carried a non-null rescued data column',
    ingested_at             TIMESTAMP NOT NULL,
    environment             STRING
)
USING DELTA
COMMENT 'ENHANCEMENT: file level ingestion lineage for the bronze layer'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.columnMapping.mode = 'name');

-- =====================================================================================
-- Convenience views for monitoring
-- =====================================================================================
CREATE OR REPLACE VIEW ${fw_catalog}.${fw_audit_schema}.vw_latest_batch_status AS
SELECT
    batch_id,
    layer,
    COUNT(*)                                                       AS task_count,
    SUM(CASE WHEN job_status = 'SUCCEEDED' THEN 1 ELSE 0 END)      AS succeeded,
    SUM(CASE WHEN job_status = 'FAILED'    THEN 1 ELSE 0 END)      AS failed,
    SUM(CASE WHEN job_status = 'RUNNING'   THEN 1 ELSE 0 END)      AS still_running,
    SUM(COALESCE(records_inserted, 0))                             AS records_inserted,
    SUM(COALESCE(records_rejected, 0))                             AS records_rejected,
    MIN(task_start_timestamp)                                      AS batch_start,
    MAX(task_end_timestamp)                                        AS batch_end
FROM ${fw_catalog}.${fw_audit_schema}.job_run_audit
GROUP BY batch_id, layer;

CREATE OR REPLACE VIEW ${fw_catalog}.${fw_audit_schema}.vw_dq_rule_trend AS
SELECT
    catalog_name,
    schema_name,
    table_name,
    column_name,
    rule_id,
    severity,
    DATE(evaluated_at)              AS run_date,
    SUM(rows_evaluated)             AS rows_evaluated,
    SUM(rows_failed)                AS rows_failed,
    CASE WHEN SUM(rows_evaluated) = 0 THEN NULL
         ELSE 100.0 * (1 - SUM(rows_failed) / SUM(rows_evaluated)) END AS pass_pct
FROM ${fw_catalog}.${fw_audit_schema}.dq_result_detail
GROUP BY catalog_name, schema_name, table_name, column_name, rule_id, severity, DATE(evaluated_at);
