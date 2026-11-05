#!/usr/bin/env bash
# scripts/bench_all.sh - todas as medições do relatório, numa só máquina, com resultados em $RESULTS (default results-hugo).
# Uso: scripts/bench_all.sh [fase ...]     fases: info main cache ab scaling check   (sem argumentos: todas, por ordem)
# Cada fase intercala as configurações (A, B, C, A, B, C, ...) para que todas apanhem as mesmas condições da máquina.
set -u
cd "$(dirname "$0")/.."
export RESULTS=${RESULTS:-results-hugo}
mkdir -p "$RESULTS"
FINAL="materialize,agg_first,cache_agg"
P24="spark.sql.shuffle.partitions=24"
clean() { rm -rf output/bench/*/m1_trips; }     # cópias de 1,5 GB das viagens limpas, regeneráveis

info() {   # hardware e configuração, para a secção de setup do relatório
    { date; uname -a
      sysctl -n machdep.cpu.brand_string hw.physicalcpu hw.logicalcpu hw.memsize 2>/dev/null || lscpu
      docker info --format 'Docker VM: {{.NCPU}} CPUs, {{.MemTotal}} bytes'
      curl -s localhost:8080/json/ | python3 -c 'import sys,json; d=json.load(sys.stdin); print("workers", d["aliveworkers"], [(w["cores"], w["memory"]) for w in d["workers"] if w["state"]=="ALIVE"])'
    } | tee "$RESULTS/hardware.txt"
}
main() {   # pipeline completo: baseline, OPT-1, OPT-1+2, final (OPT-1+2+4+5)
    for r in 1 2 3; do
        echo "== main ronda $r"
        scripts/run.sh baseline "" 2019 1
        scripts/run.sh materialize "materialize" 2019 1
        scripts/run.sh mat_agg "materialize,agg_first" 2019 1
        CONF=$P24 scripts/run.sh final "$FINAL" 2019 1
        clean
    done
}
cache() { scripts/run.sh cache "cache" 2019 1; }      # OPT-3: falhou com OOM no portátil do Tomás
ab() {     # OPT-4 e OPT-5 isoladas, só O2 e O3
    for r in 1 2 3; do
        echo "== ab ronda $r"
        OPS=O2,O3 scripts/run.sh ab_A "materialize,agg_first" 2019 1
        OPS=O2,O3 scripts/run.sh ab_B "$FINAL" 2019 1
        OPS=O2,O3 CONF=$P24 scripts/run.sh ab_C "$FINAL" 2019 1
        OPS=O2,O3 CONF=$P24 scripts/run.sh ab_D "materialize,agg_first" 2019 1
        clean
    done
}
scaling() { scripts/scaling.sh 3; clean; }
check() {  # resultados iguais à baseline + tabela de medianas
    docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
        --driver-memory 1g --executor-memory 1200m /project/scripts/compare.py materialize mat_agg final ab_C \
        2>&1 | grep -E "^RESULT" | tee "$RESULTS/compare.txt"
    python3 scripts/summarize.py "$RESULTS/metrics"
}

phases=${*:-info main cache ab scaling check}
for f in $phases; do echo "######## $f $(date +%H:%M)"; $f; done
echo "######## fim $(date +%H:%M)"
