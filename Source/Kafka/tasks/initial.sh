#!/bin/bash

REPLICATION_FACTOR="1"
BROKER="kafka-broker-01"
KAFKA_BIN="/opt/kafka/bin/kafka-topics.sh"
BOOTSTRAP="localhost:9092"

docker exec -it $BROKER $KAFKA_BIN \
	--create --bootstrap-server $BOOTSTRAP \
	--topic api_price_requests \
	--partitions 4 \
	--replication-factor $REPLICATION_FACTOR \
	--config segment.ms=82800000 \
	--config retention.ms=3600000

docker exec -it $BROKER $KAFKA_BIN \
	--create --bootstrap-server $BOOTSTRAP \
	--topic api_price_results \
	--partitions 4 \
	--replication-factor $REPLICATION_FACTOR \
	--config segment.ms=82800000 \
	--config retention.ms=3600000


docker exec -it $BROKER $KAFKA_BIN \
	--create --bootstrap-server $BOOTSTRAP \
	--topic monitoring_events \
	--partitions 4 \
	--replication-factor $REPLICATION_FACTOR \
	--config segment.ms=82800000 \
	--config retention.ms=3600000

exit 0