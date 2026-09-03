CREATE DATABASE IF NOT EXISTS Kafka;

CREATE TABLE IF NOT EXISTS Kafka.api_performance_logs
(
    timestamp DateTime64,
    user_id UUID,
    request_id UUID,
    response_time_ms Float32,
    status String,
    final_price Float32,
    request_size Int32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker-01:9092',
    kafka_topic_list = 'monitoring_events',
    kafka_group_name = 'clickhouse_monitoring_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2;