CREATE TABLE IF NOT EXISTS job_runs (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL UNIQUE,
    topic_name VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    total_steps INTEGER NOT NULL,
    processed_steps INTEGER NOT NULL,
    progress_percentage INTEGER NOT NULL,
    retry_count INTEGER NOT NULL,
    error_message VARCHAR(4000),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE,
    finished_at TIMESTAMP WITH TIME ZONE,
    processing_time_ms BIGINT
);

CREATE INDEX IF NOT EXISTS idx_job_runs_status ON job_runs(status);
CREATE INDEX IF NOT EXISTS idx_job_runs_created_at ON job_runs(created_at);
