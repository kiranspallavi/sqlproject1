-- Create audit tables for tracking ETL operations

CREATE TABLE IF NOT EXISTS audit_etl_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    process_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    operation_type VARCHAR(20) NOT NULL, -- INSERT, UPDATE, DELETE, TRUNCATE
    records_affected INTEGER DEFAULT 0,
    start_time DATETIME NOT NULL,
    end_time DATETIME,
    status VARCHAR(20) NOT NULL, -- RUNNING, SUCCESS, FAILED
    error_message TEXT,
    created_by VARCHAR(100) DEFAULT 'ETL_PROCESS'
);

CREATE TABLE IF NOT EXISTS audit_data_quality (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    quality_check VARCHAR(100) NOT NULL,
    check_result VARCHAR(20) NOT NULL, -- PASS, FAIL, WARNING
    records_checked INTEGER DEFAULT 0,
    failed_records INTEGER DEFAULT 0,
    check_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    details TEXT
);

CREATE TABLE IF NOT EXISTS audit_record_changes (
    change_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name VARCHAR(100) NOT NULL,
    record_id INTEGER NOT NULL,
    operation VARCHAR(20) NOT NULL, -- INSERT, UPDATE, DELETE
    old_values TEXT,
    new_values TEXT,
    changed_by VARCHAR(100) DEFAULT 'ETL_PROCESS',
    change_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for audit tables
CREATE INDEX IF NOT EXISTS idx_audit_etl_log_process ON audit_etl_log(process_name);
CREATE INDEX IF NOT EXISTS idx_audit_etl_log_table ON audit_etl_log(table_name);
CREATE INDEX IF NOT EXISTS idx_audit_data_quality_table ON audit_data_quality(table_name);
CREATE INDEX IF NOT EXISTS idx_audit_record_changes_table ON audit_record_changes(table_name);