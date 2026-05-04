CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.ingestion_log (
    id BIGSERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    layer_name TEXT NOT NULL,
    service_type TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    source_url TEXT NOT NULL,
    local_path TEXT,
    minio_bucket TEXT,
    minio_object_key TEXT,
    file_size_bytes BIGINT,
    status TEXT NOT NULL,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ingestion_log_layer_period
ON metadata.ingestion_log (layer_name, year, month, service_type);
