CREATE DATABASE IF NOT EXISTS MV;

CREATE MATERIALIZED VIEW MV.api_performance_logs_mv TO Monitoring.api_performance_logs AS
SELECT *
FROM Kafka.api_performance_logs;

