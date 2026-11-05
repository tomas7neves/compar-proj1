#!/usr/bin/env bash
# scripts/scaling.sh - escalabilidade da melhor configuração, em rondas intercaladas.
#   cluster: 3, 2, 1 workers com o ano inteiro; dados: fração 0.1 e 0.2 com 3 workers (100% = sc_w3).
# Uso: scripts/scaling.sh [rondas]
set -u
cd "$(dirname "$0")/.."
OPT="materialize,agg_first,cache_agg"
export CONF="spark.sql.shuffle.partitions=24"
workers() {   # escala o cluster e espera que o master veja os N workers vivos
    (cd docker && docker compose up -d --scale spark-worker="$1" spark-worker >/dev/null 2>&1)
    until [ "$(curl -s -m 5 localhost:8080/json/ >/dev/null; echo ok)" = ok ] && [ "$(curl -s localhost:8080/json/ | python3 -c 'import sys,json; print(json.load(sys.stdin)["aliveworkers"])')" = "$1" ]; do sleep 5; done
}
for r in $(seq 1 "${1:-3}"); do
    echo "== ronda $r"
    for w in 3 2 1; do workers "$w"; scripts/run.sh "sc_w$w" "$OPT" 2019 1; done
    workers 3
    for f in 0.1 0.2; do FRACTION=$f scripts/run.sh "sc_f$f" "$OPT" 2019 1; done
done
workers 3
