# src/taxi/o3_forecast.py
# O3 - features de previsão de procura por (zona, dia da semana, hora): média e variância históricas,
# lag de 4 semanas (self-join), índice de procura e flag > 2.0, anotado com service_zone.
# Explicação passo a passo em notebooks/o3-explorar.ipynb.
import calendar, collections, datetime
from pyspark.sql import functions as F
from taxi import m1_clean as m1

MIN_MEAN = 1.0      # só há alerta se o balde tiver em média >= 1 viagem/hora (decisão do grupo)


def run(trips, zones, a, opt):
    spark = trips.sparkSession
    pickup = F.col("tpep_pickup_datetime")

    # 1) viagens por (zona, dia, hora); só timestamps dentro do mês do ficheiro
    c = (trips.filter((F.year(pickup) == F.col("year")) & (F.month(pickup) == F.col("month")))
              .groupBy("PULocationID", F.to_date(pickup).alias("pickup_date"), F.hour(pickup).alias("hour_of_day"))
              .agg(F.count("*").alias("trip_count"))
              .withColumn("day_of_week", F.dayofweek("pickup_date")))          # 1 = domingo

    # 2) média e variância por balde, contando as horas sem viagens (x = 0):
    #    média = Σx/N, var = (Σx² − N·média²)/(N−1), N = nº de dias desse dia da semana no período
    dias = collections.Counter(datetime.date(y, m, d).isoweekday() % 7 + 1          # domingo = 1, como o Spark
                               for y in a.years for m in m1.months(a.months)
                               for d in range(1, calendar.monthrange(y, m)[1] + 1))
    n_days = spark.createDataFrame(sorted(dias.items()), "day_of_week INT, n_days INT")
    hist = (c.groupBy("PULocationID", "day_of_week", "hour_of_day")
             .agg(F.sum("trip_count").alias("sum_x"), F.sum(F.col("trip_count") ** 2).alias("sum_x2"))
             .join(n_days, "day_of_week")
             .withColumn("hist_mean", F.col("sum_x") / F.col("n_days"))
             .withColumn("hist_var", F.try_divide(F.col("sum_x2") - F.col("n_days") * F.col("hist_mean") ** 2,
                                                  F.col("n_days") - 1))
             .drop("sum_x", "sum_x2", "n_days"))

    # 3) lag de 4 semanas: self-join com a data avançada 28 dias.
    #    dia de há 28 dias carregado mas sem viagens -> 0; fora do período -> null
    carregados = [y * 12 + m for y in a.years for m in m1.months(a.months)]
    d28 = F.date_sub("pickup_date", 28)
    lag = c.select("PULocationID", "hour_of_day",
                   F.date_add("pickup_date", 28).alias("pickup_date"), F.col("trip_count").alias("lag_4w"))
    f = (c.join(lag, ["PULocationID", "pickup_date", "hour_of_day"], "left")
          .withColumn("lag_4w", F.when((F.year(d28) * 12 + F.month(d28)).isin(carregados),
                                       F.coalesce("lag_4w", F.lit(0)))))

    # 4) índice de procura, flag > 2 (só com volume mínimo: baldes quase vazios davam índices enormes)
    #    e anotação com service_zone
    pz = zones.select(F.col("LocationID").alias("PULocationID"), F.col("Zone").alias("zone"),
                      F.col("Borough").alias("borough"), "service_zone")
    res = (f.join(hist, ["PULocationID", "day_of_week", "hour_of_day"])
            .withColumn("demand_index", F.try_divide(F.col("trip_count"), F.col("hist_mean")))
            .withColumn("high_demand", F.coalesce((F.col("demand_index") > 2.0)
                                                  & (F.col("hist_mean") >= MIN_MEAN), F.lit(False)))
            .join(pz, "PULocationID"))

    # 5) escrever: uma pasta por service_zone
    res.write.mode("overwrite").partitionBy("service_zone").parquet(f"{a.output}/o3")
    return res
