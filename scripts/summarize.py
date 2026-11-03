# scripts/summarize.py - junta as métricas de results/metrics/ numa tabela por configuração (mediana das corridas).
# Uso: python3 scripts/summarize.py [results/metrics]  -> imprime Markdown e grava results/summary.md
import glob, json, statistics, sys
from collections import defaultdict

d = sys.argv[1] if len(sys.argv) > 1 else "results/metrics"
runs = defaultdict(list)
for f in sorted(glob.glob(f"{d}/*.json")):
    m = json.load(open(f))
    runs[m["run"]].append(m)

OPS = ["M1", "M2", "M3", "O2", "O3"]
med = lambda xs: statistics.median(xs) if xs else float("nan")
mb = lambda b: b / 1e6

out = ["| config | n | total (s) | " + " | ".join(f"{o} (s)" for o in OPS) + " | shuffle write (MB) | input records (M) |",
       "|---|---|---|" + "---|" * len(OPS) + "---|---|"]
for name, ms in runs.items():
    tot = med([m["total_s"] for m in ms])
    per = [med([m["runs"][o]["wall_time_s"] for m in ms if o in m["runs"]]) for o in OPS]
    sw = med([sum(r.get("shuffle_write_bytes", 0) for r in m["runs"].values()) for m in ms])
    ir = med([sum(r.get("input_records", 0) for r in m["runs"].values()) for m in ms])
    out.append(f"| {name} | {len(ms)} | {tot:.0f} | " + " | ".join(f"{p:.0f}" for p in per)
               + f" | {mb(sw):.1f} | {ir / 1e6:.0f} |")
out.append("")
out.append("Corridas individuais (total s): " + "; ".join(
    f"{n}: " + ", ".join(f"{m['total_s']:.0f}" for m in ms) for n, ms in runs.items()))
txt = "\n".join(out)
print(txt)
open("results/summary.md", "w").write(txt + "\n")
