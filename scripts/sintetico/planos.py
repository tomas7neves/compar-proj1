# scripts/sintetico/planos.py - corre M1 + M3/O2/O3 sobre data/sintetico e guarda o explain(mode="formatted")
# em results/plans/sintetico-<op>.txt. O2 e O3 vêm da função run() da última célula dos notebooks.
# Uso: correr antes o gerar_dados.py; depois
#   docker compose exec spark-master /opt/spark/bin/spark-submit /project/scripts/sintetico/planos.py
import sys, json, contextlib, io, re, calendar, collections, datetime
from types import SimpleNamespace
sys.path.insert(0, "/project/src")
from pyspark.sql import SparkSession, Window, functions as F
import taxi.m1_clean as m1
from taxi import m3_zones
spark = SparkSession.builder.master("local[2]").getOrCreate()

class NoProf:
    def profile(self, name): return contextlib.nullcontext()

def fn_from_nb(path):
    nb = json.load(open(path))
    src = [''.join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"][-2].split("final = run")[0]
    g = {"F": F, "Window": Window, "m1": m1, "calendar": calendar, "collections": collections, "datetime": datetime}
    exec(src, g); return g["run"]

def plan(df):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf): df.explain(mode="formatted")
    return buf.getvalue()

ops = {"M3": (m3_zones.run, "1-6"), "O2": (fn_from_nb("/project/notebooks/o2-explorar.ipynb"), "1-9"),
       "O3": (fn_from_nb("/project/notebooks/o3-explorar.ipynb"), "1-3")}
for name, (run, months) in ops.items():
    a = SimpleNamespace(input="/project/data/sintetico/raw", zones="/project/data/sintetico/taxi_zone_lookup.csv", output="/tmp/sintetico-out",
                        years=[2019], months=months, fraction=1.0)
    trips, zones = m1.run(spark, a, set(), NoProf())
    p = plan(run(trips, zones, a, set()))
    open(f"/project/results/plans/sintetico-{name}.txt", "w").write(p)
    nodes = collections.Counter(re.findall(r"^\(\d+\) (\w+)", p, re.M))
    print(name, "| leituras de viagens (FileScan parquet):", len(re.findall(r"^\(\d+\) Scan parquet", p, re.M)),
          "|", dict((k, v) for k, v in nodes.items() if k in ("Exchange", "BroadcastExchange", "SortMergeJoin", "BroadcastHashJoin",
                                                             "BroadcastNestedLoopJoin", "CartesianProduct", "Window", "WindowGroupLimit")))
