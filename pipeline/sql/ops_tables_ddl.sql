-- ops_tables_ddl.sql
-- Creates operational tables in olist_ops dataset
-- Run once during setup (idempotent – uses CREATE TABLE IF NOT EXISTS)

-- ─── 1. Pipeline run log ─────────────────────────────────────────────────────
-- One row per (batch_id, table_name) – tracks load metrics per execution
CREATE TABLE IF NOT EXISTS `{project}.olist_ops.pipeline_run_log`
(
  batch_id          STRING    NOT NULL  OPTIONS(description='Unique run identifier (UUID)'),
  run_type          STRING    NOT NULL  OPTIONS(description='FULL or INCREMENTAL'),
  table_name        STRING    NOT NULL  OPTIONS(description='Target curated table name'),
  records_read      INT64               OPTIONS(description='Rows read from raw'),
  records_inserted  INT64               OPTIONS(description='New rows written to curated'),
  records_updated   INT64               OPTIONS(description='Rows updated (incremental only)'),
  records_skipped   INT64               OPTIONS(description='Rows excluded (nulls, dupes, etc.)'),
  status            STRING              OPTIONS(description='SUCCESS or FAILED'),
  error_message     STRING              OPTIONS(description='Error detail if status=FAILED'),
  run_timestamp     TIMESTAMP NOT NULL  OPTIONS(description='UTC time the batch started'),
  duration_seconds  FLOAT64             OPTIONS(description='Wall-clock seconds for this table')
)
OPTIONS(
  description='Pipeline execution log – one row per batch × table'
);


-- ─── 2. Data quality results ─────────────────────────────────────────────────
-- One row per DQ check per batch execution
CREATE TABLE IF NOT EXISTS `{project}.olist_ops.dq_results`
(
  batch_id          STRING    NOT NULL  OPTIONS(description='Links back to pipeline_run_log'),
  check_name        STRING    NOT NULL  OPTIONS(description='Short identifier for the check'),
  check_description STRING              OPTIONS(description='Human-readable description'),
  table_name        STRING              OPTIONS(description='Table the check was run on'),
  severity          STRING              OPTIONS(description='CRITICAL | ERROR | WARNING | INFO'),
  status            STRING              OPTIONS(description='PASS or FAIL'),
  affected_count    INT64               OPTIONS(description='Number of offending rows'),
  total_count       INT64               OPTIONS(description='Total rows evaluated'),
  sample_ids        STRING              OPTIONS(description='JSON array of up to 5 sample bad IDs'),
  check_timestamp   TIMESTAMP NOT NULL  OPTIONS(description='When the check ran')
)
OPTIONS(
  description='Data quality check results – one row per check × batch'
);
