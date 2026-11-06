# src/main.py - ponto de entrada do pipeline (spark-submit ... src/main.py --years 2019 --months 1-1 --ops M1)
import argparse, contextlib, importlib, io, os, time
from pyspark.sql import SparkSession
from taxi import m1_clean
from taxi.profiler import SparkProfiler

# operação -> módulo em src/taxi/. Importado só quando é pedido, para o pipeline
# correr enquanto as outras operações ainda não existem.
OPS = {"M2": "m2_temporal", "M3": "m3_zones", "O2": "o2_flows", "O3": "o3_forecast"}


def years(s):                                  # "2019" ou "2016-2020"
    a, _, b = s.partition("-")
    return list(range(int(a), int(b or a) + 1))


def parse():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="/project/data/raw")
    p.add_argument("--zones", default="/project/data/taxi_zone_lookup.csv")
    p.add_argument("--years", type=years, default=years("2019"))
    p.add_argument("--months", default="1-12")         # smoke test: --months 1-1
    p.add_argument("--fraction", type=float, default=1.0)
    p.add_argument("--ops", default="M1,M2,M3,O2,O3")
    p.add_argument("--opt", default="")                # ex: materialize,agg_first,cache_agg (ou cache)
    p.add_argument("--output", default="/project/output")
    p.add_argument("--run-name", default="baseline")
    p.add_argument("--results", default="/project/results")
    return p.parse_args()


def save_plan(df, path):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="extended")
    with open(path, "w") as f:
        f.write(buf.getvalue())


def main():
    a = parse()
    opt = set(filter(None, a.opt.split(",")))
    spark = SparkSession.builder.appName(f"taxi-{a.run_name}").getOrCreate()
    prof = SparkProfiler(spark)
    os.makedirs(f"{a.results}/plans", exist_ok=True)
    os.makedirs(f"{a.results}/metrics", exist_ok=True)
    t0 = time.time()
    trips, zones = m1_clean.run(spark, a, opt, prof)   # M1 corre sempre: as outras usam trips
    for name in a.ops.split(","):
        if name in OPS:
            op = importlib.import_module(f"taxi.{OPS[name]}")
            with prof.profile(name):
                df = op.run(trips, zones, a, opt)
            save_plan(df, f"{a.results}/plans/{a.run_name}-{name}.txt")
    total = time.time() - t0
    prof.dump(f"{a.results}/metrics/{a.run_name}-{int(t0)}.json",
              {"run": a.run_name, "args": vars(a), "opt": sorted(opt), "total_s": total,
               "conf": dict(spark.sparkContext.getConf().getAll())})
    print(f"TOTAL {total:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()
