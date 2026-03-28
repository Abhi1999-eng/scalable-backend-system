CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS stored_files (
    id BIGSERIAL PRIMARY KEY,
    upload_id VARCHAR(64) NOT NULL,
    original_filename VARCHAR(1024) NOT NULL,
    stored_filename VARCHAR(1024) NOT NULL,
    storage_path VARCHAR(2048) NOT NULL,
    content_type VARCHAR(255),
    size_bytes BIGINT NOT NULL,
    line_count BIGINT NOT NULL,
    checksum_sha256 VARCHAR(128) NOT NULL,
    status VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stored_files_upload_id ON stored_files(upload_id);

CREATE TABLE IF NOT EXISTS datasets (
    id BIGSERIAL PRIMARY KEY,
    dataset_id VARCHAR(64) NOT NULL UNIQUE,
    batch_id VARCHAR(64) NOT NULL,
    original_filename VARCHAR(1024) NOT NULL,
    stored_filename VARCHAR(1024) NOT NULL,
    storage_path VARCHAR(2048) NOT NULL,
    content_type VARCHAR(255),
    file_size_bytes BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    dataset_type VARCHAR(32) NOT NULL,
    row_count BIGINT NOT NULL,
    column_count INTEGER NOT NULL,
    error_message VARCHAR(4000),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE,
    finished_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_datasets_dataset_id ON datasets(dataset_id);
CREATE INDEX IF NOT EXISTS idx_datasets_batch_id ON datasets(batch_id);
CREATE INDEX IF NOT EXISTS idx_datasets_created_at ON datasets(created_at);

CREATE TABLE IF NOT EXISTS dataset_column_profiles (
    id BIGSERIAL PRIMARY KEY,
    dataset_id VARCHAR(64) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    column_order_index INTEGER NOT NULL,
    inferred_type VARCHAR(32) NOT NULL,
    non_null_count BIGINT NOT NULL,
    null_count BIGINT NOT NULL,
    distinct_count BIGINT NOT NULL,
    sample_value VARCHAR(1000),
    min_value VARCHAR(255),
    max_value VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_dataset_column_profiles_dataset_id
    ON dataset_column_profiles(dataset_id);

CREATE TABLE IF NOT EXISTS dataset_rows (
    id BIGSERIAL PRIMARY KEY,
    dataset_id VARCHAR(64) NOT NULL,
    row_number BIGINT NOT NULL,
    row_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dataset_rows_dataset_id ON dataset_rows(dataset_id);
CREATE INDEX IF NOT EXISTS idx_dataset_rows_dataset_id_row_number
    ON dataset_rows(dataset_id, row_number);

CREATE TABLE IF NOT EXISTS dataset_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_id VARCHAR(64) NOT NULL UNIQUE,
    dataset_id VARCHAR(64) NOT NULL,
    batch_id VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    progress_percentage INTEGER NOT NULL,
    total_rows BIGINT NOT NULL,
    processed_rows BIGINT NOT NULL,
    retry_count INTEGER NOT NULL,
    processing_time_ms BIGINT,
    error_message VARCHAR(4000),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE,
    finished_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_dataset_jobs_dataset_id ON dataset_jobs(dataset_id);
CREATE INDEX IF NOT EXISTS idx_dataset_jobs_batch_id ON dataset_jobs(batch_id);
CREATE INDEX IF NOT EXISTS idx_dataset_jobs_status ON dataset_jobs(status);
