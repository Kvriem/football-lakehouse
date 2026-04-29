#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JARS_DIR="${ROOT_DIR}/spark/jars"
mkdir -p "${JARS_DIR}"

download_if_missing() {
  local file_name="$1"
  local url="$2"
  if [[ -f "${JARS_DIR}/${file_name}" ]]; then
    echo "Found ${file_name}"
    return
  fi
  echo "Downloading ${file_name}"
  curl -fL "${url}" -o "${JARS_DIR}/${file_name}"
}

# Spark 3.5 + Scala 2.12 compatible coordinates.
download_if_missing \
  "iceberg-spark-runtime-3.5_2.12-1.5.2.jar" \
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"

download_if_missing \
  "nessie-spark-extensions-3.5_2.12-0.82.0.jar" \
  "https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.82.0/nessie-spark-extensions-3.5_2.12-0.82.0.jar"

download_if_missing \
  "hadoop-aws-3.3.4.jar" \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"

download_if_missing \
  "bundle-2.20.160.jar" \
  "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.160/bundle-2.20.160.jar"

download_if_missing \
  "url-connection-client-2.20.160.jar" \
  "https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.160/url-connection-client-2.20.160.jar"

echo "Spark extra jars are ready in ${JARS_DIR}"
