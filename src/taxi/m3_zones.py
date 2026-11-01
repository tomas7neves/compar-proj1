# src/taxi/m3_zones.py
# M3 - estatísticas por zona: médias e desvios por (zona, mês) e window functions por borough.
# Explicação passo a passo em notebooks/m3-explorar.ipynb.
from pyspark.sql import Window, functions as F
from taxi import m1_clean as m1


def run(trips, zones, a, opt):
    # 1) zona e borough de recolha
    pu = zones.select(F.col("LocationID").alias("PULocationID"),
                      F.col("Zone").alias("zone"), F.col("Borough").alias("borough"))
    t = trips.join(pu, "PULocationID")

    # 2) uma linha por (zona, mês). year/month vêm do nome do ficheiro (m1_clean.load)
    z = (t.groupBy("borough", "PULocationID", "zone", "year", "month")
          .agg(F.count("*").alias("trip_count"),
               F.avg("fare_amount").alias("mean_fare"),
               F.stddev("fare_amount").alias("std_fare"),
               F.avg("trip_distance").alias("mean_distance"),
               F.stddev("trip_distance").alias("std_distance")))

    # 3) grelha zona x mês: mês sem viagens = 0 (senão a zona nem é avaliada nesse mês).
    #    Feita a partir das zonas e dos argumentos: tirá-la de z faria o Spark ler as viagens 3x.
    meses = trips.sparkSession.createDataFrame(
        [(y, m) for y in a.years for m in m1.months(a.months)], "year INT, month INT")
    grelha = pu.filter(F.col("PULocationID").between(1, 263)).crossJoin(meses)
    z = (grelha.join(z.drop("borough", "zone"), ["PULocationID", "year", "month"], "left")
               .fillna(0, subset=["trip_count"])
               .withColumn("m_idx", F.col("year") * 12 + F.col("month")))   # meses seguidos entre anos

    # 4) windows
    by_month = Window.partitionBy("borough", "year", "month").orderBy(F.desc("trip_count"))
    zone_3m = Window.partitionBy("PULocationID").orderBy("m_idx").rangeBetween(-2, 0)
    borough_3m = Window.partitionBy("borough").orderBy("m_idx").rangeBetween(-2, 0)
    # mês atual fora da média: com ele lá dentro e só 3 valores, |desvio| nunca passa 1.15σ
    zone_prev = Window.partitionBy("PULocationID").orderBy("m_idx").rangeBetween(-3, -1)

    res = (z.withColumn("rank_in_borough", F.rank().over(by_month))
            # try_divide: borough sem viagens nos 3 meses dá null em vez de erro (Spark 4 tem ANSI ligado)
            .withColumn("share_3m", F.try_divide(F.sum("trip_count").over(zone_3m),
                                                 F.sum("trip_count").over(borough_3m)))
            .withColumn("rolling_mean", F.avg("trip_count").over(zone_prev))
            .withColumn("rolling_std", F.stddev("trip_count").over(zone_prev))
            # null (menos de 2 meses anteriores) -> False, como no M1
            .withColumn("volume_anomaly", F.coalesce(
                F.abs(F.col("trip_count") - F.col("rolling_mean")) > 1.5 * F.col("rolling_std"),
                F.lit(False)))
            .drop("m_idx"))

    # 5) escrever: uma pasta por borough (como o M2)
    res.write.mode("overwrite").partitionBy("borough").parquet(f"{a.output}/m3")
    return res
