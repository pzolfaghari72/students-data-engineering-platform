#!/usr/bin/env bash
set -Eeuo pipefail

: "${DEBEZIUM_TOPIC_PREFIX:?}" "${SOURCE_DB_NAME:?}" "${SOURCE_DB_SCHEMA:?}"
: "${DEBEZIUM_SCHEMA_HISTORY_TOPIC:?}" "${KAFKA_NUM_PARTITIONS:?}" "${KAFKA_LOG_RETENTION_MS:?}"

broker="localhost:${KAFKA_INTERNAL_PORT:?}"
[[ $# -eq 15 ]] || { printf 'Expected the 15 source table names.\n' >&2; exit 1; }
topics=(/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$broker")
configs=(/opt/kafka/bin/kafka-configs.sh --bootstrap-server "$broker")
# Delete-only retention preserves intermediate changes for contiguous replay.
for table in "$@"; do
    topic="$DEBEZIUM_TOPIC_PREFIX.$SOURCE_DB_NAME.$SOURCE_DB_SCHEMA.$table"
    "${topics[@]}" --create --if-not-exists --topic "$topic" --replication-factor 1 \
        --partitions "$KAFKA_NUM_PARTITIONS" --config cleanup.policy=delete \
        --config "retention.ms=$KAFKA_LOG_RETENTION_MS" --config retention.bytes=-1
    description=$("${topics[@]}" --describe --topic "$topic")
    [[ $description =~ PartitionCount:[[:space:]]+${KAFKA_NUM_PARTITIONS}[[:space:]] ]] || {
        printf 'Unexpected partition count for %s. Review the existing topic.\n' "$topic" >&2
        exit 1
    }
    policy=$("${configs[@]}" --describe --all --entity-type topics --entity-name "$topic")
    for setting in 'cleanup.policy=delete' "retention.ms=$KAFKA_LOG_RETENTION_MS" 'retention.bytes=-1'; do
        [[ $policy == *"$setting "* ]] || {
            printf 'Expected %s for %s. Review the existing topic before changing retention.\n' "$setting" "$topic" >&2
            exit 1
        }
    done
done

# Debezium needs the complete ordered schema history to resume decoding.
history=$DEBEZIUM_SCHEMA_HISTORY_TOPIC
"${topics[@]}" --create --if-not-exists --topic "$history" --replication-factor 1 --partitions 1 \
    --config cleanup.policy=delete --config retention.ms=-1 --config retention.bytes=-1
description=$("${topics[@]}" --describe --topic "$history")
policy=$("${configs[@]}" --describe --all --entity-type topics --entity-name "$history")
[[ $description =~ PartitionCount:[[:space:]]+1[[:space:]] && $policy == *'cleanup.policy=delete '* &&
   $policy == *'retention.ms=-1 '* && $policy == *'retention.bytes=-1 '* ]] || {
    printf 'Schema history must have one partition and retain the complete log.\n' >&2
    exit 1
}

for topic in "${CONNECT_CONFIG_STORAGE_TOPIC:?}" "${CONNECT_OFFSET_STORAGE_TOPIC:?}" "${CONNECT_STATUS_STORAGE_TOPIC:?}"; do
    policy=$("${configs[@]}" --describe --all --entity-type topics --entity-name "$topic")
    [[ $policy == *'cleanup.policy=compact '* ]] || {
        printf 'Expected compacted Kafka Connect storage for %s.\n' "$topic" >&2
        exit 1
    }
done
