# scripts/compare.py - compara as saídas de cada config com a baseline (exceptAll nos dois sentidos, doubles arredondados).
# Uso: docker exec spark-master spark-submit /project/scripts/compare.py <config> [<config> ...]   (pastas em output/bench/)
import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, FloatType
spark = SparkSession.builder.appName("compare").getOrCreate()
base = "/project/output/bench"
outs = ["m1_stats", "m2", "m3", "o2/flows", "o2/top20", "o3"]
def norm(df):
    return df.select([F.round(c.name, 6).alias(c.name) if isinstance(c.dataType, (DoubleType, FloatType)) else F.col(c.name)
                      for c in sorted(df.schema.fields, key=lambda f: f.name)])
for cfg in sys.argv[1:]:
    for o in outs:
        rd = spark.read.json if o == "m1_stats" else spark.read.parquet
        a = norm(rd(f"{base}/baseline/{o}")); b = norm(rd(f"{base}/{cfg}/{o}"))
        na, nb = a.count(), b.count()
        d1, d2 = a.exceptAll(b).count(), b.exceptAll(a).count()
        print(f"RESULT {cfg:12s} {o:10s} linhas {na}/{nb} só-base {d1} só-{cfg} {d2} -> {'IGUAL' if na==nb and d1==d2==0 else 'DIFERENTE'}", flush=True)
