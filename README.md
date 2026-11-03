# ComPar 26/27 — Project 1: Large-Scale Analytics (NYC Yellow Taxi, PySpark)

Tomás Neves (72901), Hugo Feteira (73658).

Operations: M1 cleaning, M2 temporal aggregation, M3 zone ranking, O2 origin–destination flows, O3 demand-forecast
features. Code in `src/taxi/`, entry point `src/main.py`, report in `report/report/report.tex`, optimisation history
in `docs/optimisation_log.md`.

## Environment

- Spark **4.2.0** (`spark:4.2.0-python3` image), standalone cluster in Docker Compose (`docker/docker-compose.yml`):
  1 master + 3 workers, each worker `--cores 2 --memory 1500M`.
- Docker Desktop VM used for the measurements: 6 CPUs, 8 GB RAM, 2 GB swap. Host: Intel i7-6920HQ, 16 GB.
- Non-default Spark settings: only `--executor-memory 1200m --driver-memory 1g` (in `scripts/run.sh`). Everything
  else is the default (AQE on, `spark.sql.shuffle.partitions=200`), unless a run sets `CONF` (see below).

## 1. Data

```bash
scripts/download_data.sh            # 2016–2020 + taxi_zone_lookup.csv into data/ (≈ 12 GB)
```
The measurements use only 2019. To download a single year, edit the `for y in ...` line of the script.

## 2. Start the cluster

```bash
cd docker && docker compose up -d --scale spark-worker=3 && cd ..
# Spark master UI: http://localhost:8080
```

## 3. Run the pipeline

```bash
scripts/run.sh <name> "<opt>" [years] [repetitions]
```
`run.sh` zips `src/taxi`, then runs
`spark-submit --master spark://spark-master:7077 --driver-memory 1g --executor-memory 1200m --py-files build/taxi.zip src/main.py --years <years> --opt <opt> --run-name <name>-<years> --output output/bench/<name>`
inside `spark-master`. Each run writes:
- metrics to `results/metrics/<name>-<years>-<t>.json`;
- the plans (`explain("extended")`) to `results/plans/`;
- the log to `results/logs/`.

Configurations measured (2019, 3 runs each):

```bash
scripts/run.sh baseline     ""                                 2019 3
scripts/run.sh materialize  "materialize"                      2019 3
scripts/run.sh mat_agg      "materialize,agg_first"            2019 3
scripts/run.sh cache        "cache"                            2019 1   # fails with OOM (OPT-3)
scripts/run.sh mat_agg_cagg "materialize,agg_first,cache_agg"  2019 3
```

`--opt` flags:

| flag | where | what it does |
|---|---|---|
| `materialize` | M1 | Writes the cleaned trips once with 32 MB row groups; the other ops read from there. |
| `cache` | M1 | `persist(MEMORY_AND_DISK)` of the cleaned trips. |
| `agg_first` | O2 | Counts by (PU, DO, year, month) before joining the zone names. |
| `cache_agg` | O2, O3 | Caches the small reused intermediates (`flows`, hourly counts). |

Other `main.py` options: `--months 1-3`, `--fraction 0.1` (sample), `--ops M1,O2`.

## 4. Summarise and check results

```bash
python3 scripts/summarize.py                     # median per configuration -> results/summary.md
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    /project/scripts/compare.py materialize mat_agg   # outputs equal to output/bench/baseline?
```
