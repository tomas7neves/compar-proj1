#!/usr/bin/env bash
set -euo pipefail
BASE=https://d37ci6vzurychx.cloudfront.net
OUT=${1:-data}
mkdir -p "$OUT/raw"
for y in 2016 2017 2018 2019 2020; do
  for m in 01 02 03 04 05 06 07 08 09 10 11 12; do
    f=yellow_tripdata_${y}-${m}.parquet
    [ -s "$OUT/raw/$f" ] || curl -fSL --retry 3 -o "$OUT/raw/$f" "$BASE/trip-data/$f"
done done
curl -fSL -o "$OUT/taxi_zone_lookup.csv" "$BASE/misc/taxi_zone_lookup.csv"
date -u +"downloaded %Y-%m-%dT%H:%MZ" > "$OUT/DOWNLOADED_AT"
