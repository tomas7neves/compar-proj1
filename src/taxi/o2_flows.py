# src/taxi/o2_flows.py
# O2 - fluxos origem -> destino: viagens por rota e mês, quota da rota nas saídas da zona,
# top-20 por borough de origem e rotas cuja quota muda de sentido entre Q1 e Q3.
# Explicação passo a passo em notebooks/o2-explorar.ipynb.
from pyspark.sql import Window, functions as F


def run(trips, zones, a, opt):
    # 1) tabela de zonas duas vezes: origem (pu_*) e destino (do_*)
    pu = zones.select(F.col("LocationID").alias("PULocationID"),
                      F.col("Zone").alias("pu_zone"), F.col("Borough").alias("pu_borough"))
    do = zones.select(F.col("LocationID").alias("DOLocationID"),
                      F.col("Zone").alias("do_zone"), F.col("Borough").alias("do_borough"))

    # 2) viagens por (rota, mês)
    if "agg_first" in opt:
        # otimização: contar pelos IDs primeiro (<= 263 x 263 rotas por mês), juntar os nomes depois.
        # O Catalyst não passa um groupBy para antes de um join: tem de ser feito à mão.
        flows = (trips.groupBy("PULocationID", "DOLocationID", "year", "month")
                      .agg(F.count("*").alias("trip_count"))
                      .join(pu, "PULocationID").join(do, "DOLocationID"))
    else:
        # baseline: os dois joins a todas as viagens, agregar depois
        flows = (trips.join(pu, "PULocationID").join(do, "DOLocationID")
                      .groupBy("PULocationID", "pu_zone", "pu_borough",
                               "DOLocationID", "do_zone", "do_borough", "year", "month")
                      .agg(F.count("*").alias("trip_count")))

    # quota da rota nas saídas da zona de origem (window sem orderBy: partição inteira)
    out = Window.partitionBy("PULocationID", "year", "month")
    flows = flows.withColumn("share", F.col("trip_count") / F.sum("trip_count").over(out))
    if "cache_agg" in opt:
        # otimização: flows é usado 4x (top-20 2x, quotas, escrita) e sem cache cada uso volta a ler as viagens.
        # São só ~330 mil linhas: cabem em memória, ao contrário das viagens (ver OPT-3)
        flows = flows.cache()

    # 3) top-20 rotas por borough de origem, no período todo carregado (decisão do grupo).
    #    row_number: exatamente 20. Desempate pelos IDs: top é calculado 2x e tem de dar as mesmas rotas
    by_borough = Window.partitionBy("pu_borough").orderBy(F.desc("total_trips"), "PULocationID", "DOLocationID")
    top = (flows.groupBy("PULocationID", "pu_zone", "pu_borough", "DOLocationID", "do_zone", "do_borough")
                .agg(F.sum("trip_count").alias("total_trips"))
                .withColumn("route_rank", F.row_number().over(by_borough))
                .filter(F.col("route_rank") <= 20))

    # 4) sentido da quota em Q1 (mar - jan) e Q3 (set - jul); inverte = sinais opostos
    quotas = (flows.join(top.select("PULocationID", "DOLocationID"), ["PULocationID", "DOLocationID"])
                   .filter(F.col("month").isin(1, 3, 7, 9))
                   .groupBy("PULocationID", "DOLocationID", "year")
                   .pivot("month", [1, 3, 7, 9]).agg(F.first("share")))      # lista fixa: sem leitura extra
    # partir do top-20 x anos: uma rota sem viagens nos 4 meses não sai do pivot e ficava toda a null
    anos = trips.sparkSession.createDataFrame([(y,) for y in a.years], "year INT")
    q = (top.select("PULocationID", "DOLocationID").crossJoin(anos)
            .join(quotas, ["PULocationID", "DOLocationID", "year"], "left")
            .fillna(0.0, subset=["1", "3", "7", "9"])                         # mês sem viagens = quota 0
            .select("PULocationID", "DOLocationID", "year",
                    F.col("1").alias("share_jan"), F.col("3").alias("share_mar"),
                    F.col("7").alias("share_jul"), F.col("9").alias("share_sep"))
            .withColumn("q1_trend", F.col("share_mar") - F.col("share_jan"))
            .withColumn("q3_trend", F.col("share_sep") - F.col("share_jul"))
            .withColumn("share_reversal", F.col("q1_trend") * F.col("q3_trend") < 0))
    res = top.join(q, ["PULocationID", "DOLocationID"])

    # 5) escrever: rotas por mês (uma pasta por borough de origem) e top-20 com a flag
    flows.write.mode("overwrite").partitionBy("pu_borough").parquet(f"{a.output}/o2/flows")
    res.write.mode("overwrite").parquet(f"{a.output}/o2/top20")
    return res
