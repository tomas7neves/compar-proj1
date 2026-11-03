#!/usr/bin/env bash
# scripts/run.sh - corre o pipeline no cluster N vezes (para medir com a mediana).
# Uso: scripts/run.sh <nome> "<opt>" [anos] [repetições]
#   ex: scripts/run.sh baseline "" 2019 3
#       scripts/run.sh agg_first "agg_first" 2019 3
# Métricas em results/metrics/<nome>-<timestamp>.json, planos em results/plans/, log em results/logs/.
set -u
cd "$(dirname "$0")/.."
name=$1; opt=${2:-}; years=${3:-2019}; reps=${4:-3}
mkdir -p build results/logs
(cd src && rm -f ../build/taxi.zip && zip -qr ../build/taxi.zip taxi -x "*__pycache__*")
for i in $(seq 1 "$reps"); do
    log="results/logs/$name-$years-$i.log"
    echo "[$(date +%H:%M:%S)] $name ($years) corrida $i/$reps"
    docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
        --driver-memory 1g --executor-memory 1200m \
        --py-files /project/build/taxi.zip /project/src/main.py \
        --years "$years" --opt "$opt" --run-name "$name-$years" \
        --output "/project/output/bench/$name" > "$log" 2>&1
    echo "    exit $? | $(grep -E '^TOTAL|Traceback' "$log" | head -1)"
done
