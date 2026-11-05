#!/usr/bin/env bash
# scripts/run.sh - corre o pipeline no cluster N vezes (para medir com a mediana).
# Uso: [CONF="k=v k2=v2"] [OPS=M1,O2] [FRACTION=0.1] [RESULTS=results] scripts/run.sh <nome> "<opt>" [anos] [repetições]
#   ex: scripts/run.sh baseline "" 2019 3
#       CONF="spark.sql.shuffle.partitions=24" scripts/run.sh sp24 "materialize" 2019 3
# Métricas em results/metrics/<nome>-<timestamp>.json, planos em results/plans/, log em results/logs/.
set -u
cd "$(dirname "$0")/.."
name=$1; opt=${2:-}; years=${3:-2019}; reps=${4:-3}
conf=(); for kv in ${CONF:-}; do conf+=(--conf "$kv"); done
R=${RESULTS:-results}
mkdir -p build "$R/logs"
(cd src && rm -f ../build/taxi.zip && zip -qr ../build/taxi.zip taxi -x "*__pycache__*")
for i in $(seq 1 "$reps"); do
    log="$R/logs/$name-$years-$(date +%H%M%S).log"
    echo "[$(date +%H:%M:%S)] $name ($years) corrida $i/$reps ${CONF:-}"
    docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
        --driver-memory 1g --executor-memory 1200m ${conf[@]+"${conf[@]}"} \
        --py-files /project/build/taxi.zip /project/src/main.py \
        --years "$years" --opt "$opt" --run-name "$name-$years" \
        --ops "${OPS:-M1,M2,M3,O2,O3}" --results "/project/$R" --fraction "${FRACTION:-1.0}" \
        --output "/project/output/bench/$name" > "$log" 2>&1
    echo "    exit $? | $(grep -E '^TOTAL|Traceback' "$log" | head -1)"
done
