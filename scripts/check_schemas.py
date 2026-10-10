import collections, glob, sys
import pyarrow.parquet as pq
COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
        "passenger_count", "trip_distance", "RatecodeID", "payment_type",
        "fare_amount", "tip_amount", "total_amount"]
types = collections.defaultdict(lambda: collections.defaultdict(list))
total = 0
for f in sorted(glob.glob(sys.argv[1] + "/*.parquet")):
    pf = pq.ParquetFile(f)
    total += pf.metadata.num_rows
    s = pf.schema_arrow
    for c in COLS:
        t = str(s.field(c).type) if c in s.names else "MISSING"
        types[c][t].append(f[-15:-8])
for c, d in types.items():
    print(c, {t: f"{len(v)} ficheiros (ex: {v[0]})" for t, v in d.items()})
print("linhas totais:", total)
