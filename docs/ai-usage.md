# AI usage (draft for the report's extra page)

Tool: **Claude Code** (Anthropic CLI agent, model Claude Opus) running on Tomás's laptop, with shell access to the
repository and to the local Docker Spark cluster. Hugo used his own AI agent (details: TODO Hugo).
Sessions: 2026-10-08 → 2026-11-03 (transcripts kept locally; the prompts below are quoted from them, translated from
Portuguese, originals in *italics*).

## Did we develop an agent?

No custom agent was built. We used Claude Code as-is, with a few persistent rules (Claude Code "memory" files) that
we set during the project:

1. **Learning mode** (from 2026-10-22): the AI gives code in small blocks and explains every line and why it was
   chosen. Tomás runs each block in a notebook and checks the output before it goes into `src/taxi/`.
   *"eu quero que tu me dês o código e eu vou fazendo mas vais-me explicando tudo e o motivo de cada coisa"*
2. **Commits are the students' own**: the AI never commits with its own signature.
   *"O claude não pode dar commits neste projeto assinados por ele, tem de ser por mim"*
3. **Follow the course material**: solutions must use what was taught in the lectures and labs.
   *"é importante seguirmos as regras dos labs e das teóricas"*
4. **Never commit the dataset**; personal notes stay local (gitignored).
5. **Respect the hardware limits**: Docker VM capped at 6 CPU / 8 GB after 12 GB froze the laptop. The AI must not
   stop other workloads on the machine without asking.

## Development phase: what the AI did

| Task | How AI was used | Who validated |
|---|---|---|
| Planning | Read the assignment, lectures and labs. Wrote a step-by-step plan with a work split. | Tomás + Hugo |
| Theory study | Built a study page (Lec01–05, map/reduce/shuffle, parallel patterns) before coding. | Tomás |
| Environment | Docker Compose cluster (1 master + 3 workers), downloaded 2019 data, debugged worker registration. | Tomás |
| M1, M2, O2 code | Wrote code in small blocks with an explanation of each line. Tomás ran each block in a notebook. | Tomás |
| M3, O3 code | Hugo wrote these with his own agent. Claude Code audited them with synthetic tests and found 2 bugs in O2; Hugo fixed them. | Tomás + Hugo |
| Pipeline files | Moved the notebook code into `o2_flows.py` / `o3_forecast.py`. Added the low-volume threshold. | Tomás |

Example prompts (development):
- "I received the project assignment. Read everything we have from the lectures and labs and make an execution plan."
- "Now I want to change the approach: give me the code, I will do it, but explain everything and the reason for each thing."
- "Fetch Hugo's new commits [...] then do a full audit of what was done."
- "Explain it to me step by step, by notebook block." (*"faz isso por blocos"*)
- "Shouldn't January also count December and November of the previous year?" (Tomás's question about the M3 window)
- Design decisions were made by us, not by the AI. For example, the top-20 period:
  *"eu digo período completo: se o enunciado não diz ano, ficamos assim como o Hugo escolheu"*. Another example is
  reporting and fixing the false alerts in near-empty zones: *"dizemos no relatório esse acontecimento e para
  resolvermos fizemos y"*.

## Optimisation phase: what the AI did

What we used it for:
- Running benchmarks on the Docker cluster. `scripts/run.sh` runs the pipeline N times;
  `scripts/summarize.py` takes the median of N runs per configuration.
- Reading `explain(mode="extended")` plans and profiler metrics (tasks, shuffle bytes, input records, CPU time,
  partition sizes) to find each bottleneck **before** changing code.
- Implementing each optimisation behind an `--opt` flag, so baseline and optimised versions stay comparable.
- Checking that results are unchanged (`scripts/compare.py`: `exceptAll` against the baseline output).

Prompts (optimisation):
- "Do you want to test it and see if we can optimise something?"
- "Start running what you need, I'm going to sleep; run everything needed so tomorrow we only write the report."
- "And now what? Do you want to measure the rest?" / "Yes, run the cache once."
- "More optimisation strategies to present in the report?" / "Yes, go ahead with OPT-4."

Analysis behind each optimisation (data → hypothesis):

| Opt | Data that pointed to it | Analysis |
|---|---|---|
| OPT-1 materialize | Profiler: read stage max task 75 s vs median 2.7 s (28× skew). `spark_partition_id()`: all rows of a file in one partition. | TLC files have 1 row group. Read parallelism = number of files, and every op re-reads them (~10 reads per run). |
| OPT-2 agg_first | O2 plan: two BroadcastHashJoins over every trip, 4 string columns in the shuffle. | Aggregating by IDs first shrinks the rows to ≤ 263² per month before the joins. Catalyst does not push a groupBy below a join. |
| OPT-3 cache (rejected) | Same as OPT-1. | Measured an OOM (8 executors lost). The cached trips do not fit in 4.5 GB of executor heap. |
| OPT-4 cache_agg | Plans: O2 scans the trips 4×, O3 3× (intermediates reused but recomputed). | Cache only the small aggregates (330 k / 1 M rows). Measured: input records 188.8 M → 80.8 M. Side effect: 200-partition cached data → ~2–3 k tasks. |

The AI also caught measurement problems we would otherwise have reported wrongly:
- Noise from other containers on the same VM (±40 % wall time), so we use medians, executor CPU time and shuffle bytes.
- A run on a less loaded machine that would have exaggerated OPT-4's gain, so we re-measured the previous config in
  the same conditions.

## Limits / what we checked ourselves
- Every number in the report comes from `results/metrics/*.json`, produced by our profiler, not from the AI.
- The AI's first synthetic test for O3 was wrong (13 Mondays instead of 12 in Q1 2019); we found and fixed it.
- Hugo's notebooks were written with his agent and never executed by him. We audited them before merging.
