# AI usage (draft for the report's extra page)

Tool: **Claude Code** (Anthropic CLI agent, model Claude Opus) running on Tomás's laptop, with shell access to the
repository and to the local Docker Spark cluster. Hugo used his own Claude Code sessions (see "Hugo's AI usage" below).
Sessions: 2026-10-08 → 2026-11-05 (transcripts kept locally; the prompts below are quoted from them, translated from
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
| Scripts | Wrote `run.sh`, `bench_all.sh`, `scaling.sh`, `compare.py` and `summarize.py`. | Tomás + Hugo (39 runs) |
| Report text | Drafted and rewrote `report.tex` from our notes and results (the first version read as "confusing and not very human", so it was rewritten); checked each requirement of the assignment against the report. | Tomás |

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
- Claude Code (Tomás's sessions) first wrote a synthetic O3 test expecting 13 Mondays in Q1 2019 (there are 12). The test failed and we fixed the expected value. Separately, the text of `o3-explorar.ipynb` said 13 and Hugo corrected it.
- Hugo's notebooks were written with his agent and never executed by him. We audited them before merging.

## Hugo's AI usage

Tool: **Claude Code** (Anthropic CLI agent), model **Claude Opus 5.5** (`claude-opus-5-5`). It ran on Hugo's MacBook
with shell access to his clone of the repository and to Docker, using the `spark:4.2.0-python3` image in local mode,
not the 3-worker cluster. Sessions: 2026-10-29 → 2026-11-03. Prompts are quoted from the session transcript,
translated to English, originals (typos included) in *italics*.

### Did Hugo develop an agent?

No custom agent was built. Claude Code was used as-is, with these fixed rules:

1. **Hugo's global instruction file** (`~/.claude/CLAUDE.md`, in Portuguese, applies to all his projects):
   - commits and PRs are never signed by Claude (no `Co-Authored-By`, no "Generated with");
   - sub-agents run in their own git worktree on an `agent/<task>` branch, at most 3 at a time, and never merge or push;
   - read-only review agents need no worktree;
   - tests are run only when really needed;
   - Claude asks before running `git init`.
2. **Built-in Claude Code rules** that shaped the work: commit only when asked, and never on the default branch
   (hence the branches `m3-o2-o3`, `fix-o2` and `hugo-report`); confirm before irreversible actions.
3. **One built-in sub-agent** (general-purpose, read-only, no worktree) was launched once, at Hugo's request, to
   review the first M3 version.
4. A memory plugin (`claude-mem`) was installed but held no notes for this project. No project `CLAUDE.md` was used.

### What the AI did

| Task | How the AI was used | Who validated |
|---|---|---|
| Environment on Hugo's machine | Cloned the repo, pulled `spark:4.2.0-python3`, built the Jupyter image. | — |
| Project status | Read the assignment PDF and the code, and listed what was done and what was missing. | Hugo (read it) |
| M3, first attempt | Wrote `m3_zones.py`. The lab repositories linked in the lab PDFs were not reachable (GitHub 404), so it followed the lecture slides (Lec03/04) and the M1/M2 code. A review sub-agent found two issues: a zone that drops to 0 trips is never flagged, and σ=0 flags any change. **Hugo then asked to revert everything**, and the file was deleted. | Review sub-agent |
| M1/M2 explanation | Wrote a Markdown file explaining M1 and M2 in detail. It was never committed and is no longer in the repo. | Hugo (read it) |
| M3, O2, O3 notebooks | Wrote `m3-explorar`, `o2-explorar` and `o3-explorar` block by block, each with a Markdown explanation of the code. Converted the final M3 block into `src/taxi/m3_zones.py`. | Synthetic tests (below), then Tomás's audit |
| Synthetic tests | Ran the code of every notebook cell with `spark-submit` in a local-mode container, on small hand-made data with known answers (a missing month, a spike, a timestamp from 2008, routes whose share rises in Q1 and falls in Q3). They found 3 bugs before the first commit: division by zero under ANSI mode, the M3 grid reading the trips 3×, and a false claim in the notebook text that Spark reused intermediates. | AI (expected values worked out by hand) |
| O2 fixes (`fix-o2`) | Fixed the two bugs found by **Tomás's audit**, which the AI's tests had not caught: null shares for top-20 routes with no trips in Jan/Mar/Jul/Sep, and a non-deterministic `row_number`. Added the minimal test Tomás specified. Also generated `explain()` plans on synthetic Parquet data (`scripts/sintetico/`, `results/plans/sintetico-*`). | Tomás's test case; AI ran it |
| `docs/hugo-respostas.md` | Answered Tomás's audit questions (Q8, Q9) as a draft for Hugo to review. Numbers there come from Tomás's audit, not from the AI. | Hugo (to review) |
| Report and this section | Reviewed the report text about M3/O3, wrote this disclosure, and closed the open items. | Hugo + Tomás |

The AI did **not** write or change the profiler, `main.py`, M1, M2, the `--opt` optimisations or any measurement.

### Hugo's prompts (all of them, in order)

- "Clone it." (*"clona"*). Then the URL `https://github.com/tomas7neves/compar-proj1.git`.
- "Install the dependencies needed to run the project." (*"instala as dependencias necessria para correr o projeto"*)
- "Read the project PDF, analyse the code and tell me which parts are already done."
  (*"le o pdf do projeto e analisa o codigo e dizme que partes ja esao faitas"*)
- "Do M3. The code has to agree with what was done before and has to use code from the labs."
  (*"faz o M3 o coidog tem de estar de acordo com o feito anteriormnete e temde usar codigo dos labs"*)
- "Launch an agent that will assess whether M3 is well done, agrees with what was done before and agrees with the
  lab PDFs." (*"lanca um agente que vai avaliar se o M3 esta bem feito e esta de acordo com o feito anteriormente e
  se esta de acordo com os pdfs dos labs"*)
- "Undo everything you did, go back to how it was on GitHub." (*"volta atras com tudo o que fizeste volta a como
  estava no github"*)
- "I want you to create an md explaining how M1 and M2 work in detail and what was implemented." (*"quero que me
  cries um md a explicar como funciona o M1 e o M2 detalhadamente e o que foi implementado"*)
- "To develop M3, I want you to make a Spark notebook and do it step by step so I understand what is being done."
  (*"para deselvolveres o M3 quero que facas um notebook em spark e faz passo a passo para eu perceber o que esta a
  ser feito"*)
- "So to finish, you'll turn the Jupyter into code, right?" (*"entao para finalizar vais passar o jupiter para
  codigo e isso?"*)
- "No, first I want you to do O2 and O3 in a notebook." (*"nao primeiro quero que facas o o2 e o3 em notebook"*)
- "Commit." / "Push." (*"da commit"* / *"da push"*)
- "Is there any way for you to communicate with a Claude of a different user on a different PC?" (*"ha alguma
  amneira de te comunicares com um claude de um user diferente que esta num pc diferente?"*)

Three later requests were messages from Tomás that Hugo pasted into the session: the audit questions, the two O2
bugs, and this report task. They are Tomás's text, not Hugo's prompts.

### What Hugo validated himself, and limits

- **Hugo did not execute his notebooks before the merge.** No notebook was run in Jupyter, on the real dataset or on
  the cluster. Their code was run only by the AI, on synthetic data in local mode. The first run on real data was
  Tomás's audit (2019 Jan–Sep, sampled).
- Hugo's own checks were indirect:
  - he asked for an independent review agent before accepting M3, and rejected that first version;
  - he asked for block-by-block notebooks with explanations and a written M1/M2 explanation, to understand the code
    rather than take it as given;
  - he chose the order of the work: notebooks first, code later.
- The two O2 bugs and the wording errors in the notebooks were found by Tomás's audit, not by Hugo or by the AI's
  tests.
- The AI's first read-count claims (O2 4×, O3 3×) came from the plan before execution. The measured input records
  (2.3× and 2×, Table 1) are lower.
