# src/taxi/m2_temporal.py
# M2 - procura temporal: viagens e médias por (borough, dia da semana, hora).
from pyspark.sql import functions as F


def run(trips, zones, a, opt):
    # 1) o borough vem da tabela de zonas (as viagens só têm o número da zona)
    pu = zones.select(F.col("LocationID").alias("PULocationID"), F.col("Borough").alias("borough"))
    t = trips.join(pu, "PULocationID")

    # 2) colunas novas: dia (1 = domingo), hora e duração em minutos
    dur = (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
    t = (t.withColumn("day_of_week", F.dayofweek("tpep_pickup_datetime"))
          .withColumn("hour_of_day", F.hour("tpep_pickup_datetime"))
          .withColumn("duration_min", dur))

    # 3) uma linha por (borough, dia, hora) com o nº de viagens e as 3 médias
    res = (t.groupBy("borough", "day_of_week", "hour_of_day")
            .agg(F.count("*").alias("trip_count"),
                 F.avg("fare_amount").alias("mean_fare"),
                 F.avg("duration_min").alias("mean_duration_min"),
                 F.avg("trip_distance").alias("mean_distance")))

    # 4) escrever: uma pasta por borough
    res.write.mode("overwrite").partitionBy("borough").parquet(f"{a.output}/m2")
    return res
