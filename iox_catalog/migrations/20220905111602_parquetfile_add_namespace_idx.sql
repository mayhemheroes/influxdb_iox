-- Add index to support fetching billing information (file size by namespace).

CREATE INDEX IF NOT EXISTS parquet_file_namespace_idx ON parquet_file (namespace_id);
