# src/taxi/m1_clean.py
# M1 - ingestão e limpeza: lê os ficheiros com tipos fixos, conta por ano quantas
# viagens cada critério remove (m1_stats) e devolve as viagens limpas.
from functools import reduce
from pyspark.sql import DataFrame, functions as F

# Tipo final de cada coluna usada. passenger_count e RatecodeID são int64 até 2018-06
# e double depois (docs/schemas.txt), por isso todos os ficheiros passam por um cast.
TYPES = {
        "tpep_pickup_datetime": "timestamp", 
        "tpep_dropoff_datetime": "timestamp",
        "PULocationID": "int", 
        "DOLocationID": "int", 
        "passenger_count": "int",
        "trip_distance": "double", 
        "RatecodeID": "int", 
        "payment_type": "int",
        "fare_amount": "double", 
        "tip_amount": "double", 
        "total_amount": "double"
    }


def months(spec):
    # "1-12" -> 1..12   |   "3" -> 3
    a, _, b = spec.partition("-")
    return range(int(a), int(b or a) + 1)


def load(spark, a):
    # Um read por ficheiro (schemas diferentes), cast para TYPES, year/month do nome do ficheiro.
    dfs = []
    for y in a.years:
        for m in months(a.months):
            df = spark.read.parquet(f"{a.input}/yellow_tripdata_{y}-{m:02d}.parquet")
            dfs.append(df.select(*[F.col(c).cast(t).alias(c) for c, t in TYPES.items()],
                                 F.lit(y).alias("year"), F.lit(m).alias("month")))
    return reduce(DataFrame.unionByName, dfs)


def rules():
    # nome do critério -> condição True = a viagem é removida.
    # null_critical é nosso: nulls nas colunas usadas nas contas (RatecodeID/payment_type não são usadas).
    c = F.col
    return {
        "null_critical": reduce(lambda x, y: x | y, [c(k).isNull() for k in TYPES
                                                     if k not in ("RatecodeID", "payment_type")]),
        "fare_le_0": c("fare_amount") <= 0,
        "dist_le_0": c("trip_distance") <= 0,
        "pax_0_or_gt6": (c("passenger_count") == 0) | (c("passenger_count") > 6),
        "dropoff_le_pickup": c("tpep_dropoff_datetime") <= c("tpep_pickup_datetime"),
        "loc_out_of_range": ~c("PULocationID").between(1, 263) | ~c("DOLocationID").between(1, 263),
    }


def run(spark, a, opt, prof):
    zones = spark.read.csv(a.zones, header=True,
                           schema="LocationID INT, Borough STRING, Zone STRING, service_zone STRING")
    with prof.profile("M1"):
        raw = load(spark, a)
        if a.fraction < 1:
            raw = raw.sample(fraction=a.fraction, seed=42)
        # null -> False: sem isto, null <= 0 dá null e o filter remove a linha sem a contar.
        r = {k: F.coalesce(v, F.lit(False)) for k, v in rules().items()}
        any_bad = reduce(lambda x, y: x | y, r.values())
        # Uma só agregação = uma leitura (6x filter().count() seriam 6 leituras).
        stats = raw.groupBy("year").agg(F.count("*").alias("total"),
                                        *[F.sum(v.cast("int")).alias(k) for k, v in r.items()],
                                        F.sum(any_bad.cast("int")).alias("removed_any"))
        # 1 ficheiro: aceitável porque stats tem 1 linha por ano.
        stats.coalesce(1).write.mode("overwrite").json(f"{a.output}/m1_stats")
        trips = raw.filter(~any_bad)
    return trips, zones
