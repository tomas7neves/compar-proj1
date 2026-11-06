# ComPar 26/27 — Project 1: Large-Scale Analytics (NYC Yellow Taxi, PySpark)

Tomás Neves (72901), Hugo Feteira (73658).

Operations: M1 cleaning, M2 temporal aggregation, M3 zone ranking, O2 origin–destination flows, O3 demand-forecast
features. Code in `src/taxi/`, entry point `src/main.py`, report in `report/report/report.tex`, optimisation history
in `docs/optimisation_log.md`.

## Environment

- Spark **4.2.0** (`spark:4.2.0-python3` image), standalone cluster in Docker Compose (`docker/docker-compose.yml`):
  1 master + 3 workers, each worker `--cores 2 --memory 1500M`.
- Measurements in the report: Apple M4 (10 cores), 16 GB; Docker VM 10 CPUs, 8.2 GB (`results-hugo/`).
  First round: Intel i7-6920HQ, 16 GB; Docker VM 6 CPUs, 8 GB (`results/`).
- Non-default Spark settings: only `--executor-memory 1200m --driver-memory 1g` (in `scripts/run.sh`). Everything
  else is the default (AQE on, `spark.sql.shuffle.partitions=200`), unless a run sets `CONF` (see below).

## 1. Data

```bash
scripts/download_data.sh            # 2016–2020 + taxi_zone_lookup.csv into data/ (≈ 12 GB)
```
The measurements use only 2019. To download a single year, edit the `for y in ...` line of the script.

Source (NYC TLC Trip Record Data, https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page):
- trips: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_<YYYY>-<MM>.parquet`
- zones: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

Version: the files as published by the TLC when we downloaded them: trips on 2026-10-08, zones on 2026-10-10
(the script writes the date to `data/DOWNLOADED_AT`). 2019 = 12 files, 84,598,444 trips, 1.2 GB. The TLC occasionally republishes old files, so
a later download may differ slightly. The dataset is not in the repository.

## 2. Start the cluster

```bash
cd docker && docker compose up -d --scale spark-worker=3 && cd ..
# Spark master UI: http://localhost:8080
```

## 3. Run the pipeline

```bash
[CONF="k=v ..."] [OPS=M1,O2] [FRACTION=0.1] [RESULTS=results] scripts/run.sh <name> "<opt>" [years] [repetitions]
```
`run.sh` zips `src/taxi` into `build/taxi.zip` and then, for each repetition, runs inside `spark-master`:

```bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    --driver-memory 1g --executor-memory 1200m [--conf k=v ...] \
    --py-files /project/build/taxi.zip /project/src/main.py \
    --years <years> --opt "<opt>" --run-name <name>-<years> \
    --ops ${OPS:-M1,M2,M3,O2,O3} --results /project/${RESULTS:-results} --fraction ${FRACTION:-1.0} \
    --output /project/output/bench/<name>
```
Each run writes its metrics to `<RESULTS>/metrics/<name>-<years>-<t>.json`, the plans (`explain("extended")`) to
`<RESULTS>/plans/` and the log to `<RESULTS>/logs/`.

`--opt` flags:

| flag | where | what it does |
|---|---|---|
| `materialize` | M1 | OPT-1: writes the cleaned trips once with 32 MB row groups; the other ops read from there. |
| `agg_first` | O2 | OPT-2: counts by (PU, DO, year, month) before joining the zone names. |
| `cache` | M1 | OPT-3: `persist(MEMORY_AND_DISK)` of the cleaned trips (fails with OOM). |
| `cache_agg` | O2, O3 | OPT-4: caches the small reused intermediates (`flows`, hourly counts). |

OPT-5 is a Spark setting, not a flag: `CONF="spark.sql.shuffle.partitions=24"`.

Other `main.py` options: `--input <dir>`, `--zones <csv>`, `--years 2016-2020`, `--months 1-3`,
`--fraction 0.1` (sample), `--ops M1,O2`.

### Reproduce every measurement in the report

All the report's numbers (Apple M4) come from one command, which runs each configuration in interleaved rounds
(A, B, C, A, B, C, ...) and writes to `results-hugo/` (about 1.5 h on an Apple M4):

```bash
scripts/bench_all.sh                 # all phases, in order
scripts/bench_all.sh main check      # or only some phases
```

| phase | what it runs | report |
|---|---|---|
| `info` | hardware, Docker VM and alive workers -> `hardware.txt` | setup |
| `main` | 3 rounds of `baseline ""`, `materialize "materialize"`, `mat_agg "materialize,agg_first"`, `final "materialize,agg_first,cache_agg"` with `CONF=spark.sql.shuffle.partitions=24` | Tables 1 and 3 |
| `cache` | `cache "cache"`, 1 run (expected to fail with OOM) | OPT-3 |
| `ab` | `OPS=O2,O3`, 3 rounds of A `materialize,agg_first`; B `+cache_agg`; C `+cache_agg` with 24 partitions; D `materialize,agg_first` with 24 partitions | Table 2 |
| `scaling` | `scripts/scaling.sh 3`: final version with 3, 2, 1 workers (full year) and with `FRACTION` 0.1 and 0.2 on 3 workers | Figure 2 |
| `check` | `compare.py materialize mat_agg final ab_C` (outputs equal to the baseline) and `summarize.py` | correctness |

Each phase deletes the 1.5 GB copies in `output/bench/*/m1_trips` when it ends. The first round of measurements
(Intel laptop) is in `results/`.

## 4. Summarise and check results

```bash
python3 scripts/summarize.py results-hugo/metrics   # median per configuration -> results-hugo/summary.md
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    /project/scripts/compare.py materialize mat_agg   # outputs equal to output/bench/baseline?
```
