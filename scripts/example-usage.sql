-- Example usage of the PostHog DuckDB extension
-- Requires a built extension and a running Flight SQL server.

LOAD 'build/release/extension/posthog/posthog.duckdb_extension';
ATTACH 'hog:test?token=demo&endpoint=grpc://127.0.0.1:8815' AS remote;

SELECT 1;
SELECT * FROM remote.main.test_data;
SELECT COUNT(*) FROM remote.main.numbers;
