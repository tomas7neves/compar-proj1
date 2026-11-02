# scripts/sintetico/gerar_dados.py - dados falsos com o schema dos ficheiros TLC, para testar planos sem o dataset.
# 9 meses de 2019, 30k viagens/mês, 20 zonas (algumas inválidas de propósito, para o M1 ter o que limpar).
# Uso (pasta docker/): docker compose exec spark-master /opt/spark/bin/spark-submit /project/scripts/sintetico/gerar_dados.py
# Escreve em data/sintetico/ (ignorado pelo git, como data/).
import random, datetime, csv
from pyspark.sql import SparkSession
import os
os.makedirs("/project/data/sintetico/raw", exist_ok=True)
spark = SparkSession.builder.master("local[2]").getOrCreate()
random.seed(1)
with open("/project/data/sintetico/taxi_zone_lookup.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["LocationID", "Borough", "Zone", "service_zone"])
    for i in range(1, 266):
        b = ["Manhattan", "Queens", "Brooklyn", "Bronx", "Staten Island"][i % 5] if i > 1 else "EWR"
        w.writerow([i, b if i < 264 else "Unknown", f"Zone {i}", "Airports" if i in (1, 132, 138) else ("Yellow Zone" if b == "Manhattan" else "Boro Zone")])
zs = [1, 4, 13, 43, 48, 68, 79, 90, 100, 132, 138, 140, 141, 142, 161, 162, 163, 170, 230, 236]
for m in range(1, 10):
    rows = []
    for _ in range(30000):
        d = datetime.datetime(2019, m, random.randint(1, 28), random.randint(0, 23), random.randint(0, 59))
        dur = datetime.timedelta(minutes=random.randint(-2, 40))
        rows.append((d, d + dur, random.choice(zs), random.choice(zs), random.randint(0, 7), random.uniform(-0.5, 12),
                     float(random.choice([1, 1, 1, 2, 5])), random.choice([1, 2]), random.uniform(-3, 60), 2.0, 20.0))
    df = spark.createDataFrame(rows, "tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, PULocationID LONG, DOLocationID LONG, "
                               "passenger_count LONG, trip_distance DOUBLE, RatecodeID DOUBLE, payment_type LONG, fare_amount DOUBLE, tip_amount DOUBLE, total_amount DOUBLE")
    df.coalesce(1).write.mode("overwrite").parquet(f"/tmp/sint-m{m}")
    import glob, shutil
    shutil.copy(glob.glob(f"/tmp/sint-m{m}/*.parquet")[0], f"/project/data/sintetico/raw/yellow_tripdata_2019-{m:02d}.parquet")
print("ok")
