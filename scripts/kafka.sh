#!/usr/bin/env bash
#
# Kafka CLI helpers that run inside a broker pod.
#
#   scripts/kafka.sh topics                          list topics with partition counts
#   scripts/kafka.sh describe txn.transactions       leadership and ISR per shard
#   scripts/kafka.sh tail out.fraud-alerts           consume Avro-decoded records
#   scripts/kafka.sh count txn.transactions          records per shard
#   scripts/kafka.sh groups                          consumer groups and their lag
#   scripts/kafka.sh raw <topic>                     consume without Avro decoding

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl

POD=$(kubectl -n "${NAMESPACE}" get pod -l app.kubernetes.io/component=kafka -o name | head -1)
[[ -n "$POD" ]] || die "no Kafka pod found"
POD="${POD##*/}"

# kafka-avro-console-consumer ships in cp-schema-registry, not cp-kafka, so Avro reads run from that pod.
SR_POD=$(kubectl -n "${NAMESPACE}" get pod -l app.kubernetes.io/component=schema-registry -o name | head -1)
SR_POD="${SR_POD##*/}"

BS="localhost:9092"
BOOTSTRAP="$(kubectl -n "${NAMESPACE}" get sts "$(helm_release_name)-kafka" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="KAFKA_CONTROLLER_QUORUM_VOTERS")].value}' 2>/dev/null | sed 's/[0-9]@//g; s/9093/9092/g')"
SR="http://$(helm_release_name)-schema-registry:8081"

# -it only when there really is a terminal, so these work in scripts and CI too.
TTY_FLAGS=(-i); [[ -t 0 && -t 1 ]] && TTY_FLAGS=(-it)

k()  { kubectl -n "${NAMESPACE}" exec "$POD" -- bash -c "$1"; }
ki() { kubectl -n "${NAMESPACE}" exec "${TTY_FLAGS[@]}" "$POD" -- bash -c "$1"; }
ksr(){ kubectl -n "${NAMESPACE}" exec "${TTY_FLAGS[@]}" "$SR_POD" -- bash -c "$1"; }

case "${1:-topics}" in
  topics)
    k "kafka-topics --bootstrap-server $BS --describe | grep '^Topic:' | awk '{print \$2, \$4, \$6}' | column -t"
    ;;
  describe)
    [[ -n "${2:-}" ]] || die "usage: kafka.sh describe <topic>"
    k "kafka-topics --bootstrap-server $BS --describe --topic $2"
    ;;
  tail)
    [[ -n "${2:-}" ]] || die "usage: kafka.sh tail <topic>"
    # kafka-avro-console-consumer resolves the schema id in each record against the registry, so this prints
    # JSON rather than the raw Confluent framing.
    #
    # key.deserializer is not optional here: this project writes plain UTF-8 keys and Avro values, but the
    # tool defaults to Avro for *both*. Without it every record fails with
    #   SerializationException: Unknown magic byte!
    # which is the deserializer complaining that a key like "acct-00042" has no Confluent framing.
    ksr "kafka-avro-console-consumer --bootstrap-server $BOOTSTRAP --topic $2 \
          --property schema.registry.url=$SR \
          --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
          --property print.key=true --property key.separator=' | ' \
          --property print.partition=true \
          --isolation-level read_committed \
          --max-messages ${3:-20} --from-beginning"
    ;;
  raw)
    [[ -n "${2:-}" ]] || die "usage: kafka.sh raw <topic>"
    ki "kafka-console-consumer --bootstrap-server $BS --topic $2 --from-beginning --max-messages ${3:-10} \
          --property print.partition=true --isolation-level read_committed"
    ;;
  count)
    [[ -n "${2:-}" ]] || die "usage: kafka.sh count <topic>"
    info "records per shard (high watermark minus low watermark):"
    # kafka.tools.GetOffsetShell moved to org.apache.kafka.tools in Kafka 3.x; kafka-get-offsets is the
    # wrapper that works across both.
    k "kafka-get-offsets --bootstrap-server $BS --topic $2 --time -1 | sort -t: -k2 -n > /tmp/hi; \
       kafka-get-offsets --bootstrap-server $BS --topic $2 --time -2 | sort -t: -k2 -n > /tmp/lo; \
       paste /tmp/hi /tmp/lo | awk -F'[:\t]' '{printf \"  shard %-3s %8d records\n\", \$2, \$3-\$6}'"
    ;;
  groups)
    k "kafka-consumer-groups --bootstrap-server $BS --list" | while read -r g; do
      [[ -z "$g" ]] && continue
      echo "${BOLD}${g}${RESET}"
      k "kafka-consumer-groups --bootstrap-server $BS --describe --group '$g' 2>/dev/null | tail -n +2" | sed 's/^/  /'
    done
    ;;
  *)
    die "unknown command '${1}' - see the comments at the top of this script"
    ;;
esac
