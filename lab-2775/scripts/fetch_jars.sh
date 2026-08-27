#!/usr/bin/env bash
# Download the Flink Kafka connector jar into ./jars/ so it gets baked into the
# image (Dockerfile copies jars/ -> /opt/flink/lib).
#
# IMPORTANT: the connector version must match the base image's Flink version
# (cp-flink:2.1.3-cp2 -> Flink 2.1, hence the -2.1 suffix). Verify the artifact
# exists for your Flink version and override the URL if needed:
#   FLINK_KAFKA_CONNECTOR_URL=<url> ./scripts/fetch_jars.sh
#
# Exactly one flink-sql-connector-kafka jar may live in jars/ — two of them on
# /opt/flink/lib is a classpath conflict, so old versions are removed below.
set -euo pipefail

mkdir -p jars
DEFAULT_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.1/flink-sql-connector-kafka-5.0.0-2.1.jar"
URL="${FLINK_KAFKA_CONNECTOR_URL:-$DEFAULT_URL}"
OUT="jars/$(basename "$URL")"

if [[ ! -f "$OUT" ]]; then
  echo "Downloading $URL"
  curl -fSL "$URL" -o "$OUT"
  echo "Saved $OUT"
else
  echo "Already present: $OUT"
fi

# Drop any other flink-sql-connector-kafka jar left over from a Flink upgrade.
for old in jars/flink-sql-connector-kafka-*.jar; do
  [[ -e "$old" && "$old" != "$OUT" ]] || continue
  echo "Removing stale connector: $old"
  rm -f "$old"
done
