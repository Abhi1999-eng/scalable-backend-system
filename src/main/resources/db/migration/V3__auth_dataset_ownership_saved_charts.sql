ALTER TABLE users ADD COLUMN IF NOT EXISTS email VARCHAR(255);
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash VARCHAR(512);
ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE;

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_unique ON users(email) WHERE email IS NOT NULL;

ALTER TABLE datasets ADD COLUMN IF NOT EXISTS owner_user_id BIGINT;
CREATE INDEX IF NOT EXISTS idx_datasets_owner_user_id ON datasets(owner_user_id);

CREATE TABLE IF NOT EXISTS saved_charts (
    id BIGSERIAL PRIMARY KEY,
    chart_id VARCHAR(64) NOT NULL UNIQUE,
    owner_user_id BIGINT NOT NULL,
    dataset_id VARCHAR(64) NOT NULL,
    name VARCHAR(255) NOT NULL,
    chart_type VARCHAR(32) NOT NULL,
    x_column VARCHAR(255) NOT NULL,
    y_column VARCHAR(255),
    aggregation VARCHAR(32) NOT NULL,
    limit_value INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_saved_charts_dataset_owner ON saved_charts(dataset_id, owner_user_id);
CREATE INDEX IF NOT EXISTS idx_saved_charts_owner_user_id ON saved_charts(owner_user_id);
