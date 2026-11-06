# Optimisation log

Medições no cluster Docker local (1 master + 3 workers × 2 cores × 1500 MB, Spark 4.2.0), 2019 completo
(12 ficheiros, 84,4 M viagens), `scripts/run.sh`, mediana de 3 corridas salvo indicação.
Métricas em `results/metrics/`, planos em `results/plans/`, tabela em `results/summary.md` (`scripts/summarize.py`).

**Limitação das medições.** A VM do Docker é partilhada com contentores de outro projeto (um deles chegou a ~160 % de
CPU durante as corridas). O tempo de parede variou até ±40 % entre corridas iguais (baseline: 970, 1398, 1871 s;
M2 sem nenhuma mudança: 49 s vs 81 s). Por isso usamos a mediana e, para comparar operações, também o
**tempo de CPU dos executores** e o **shuffle write**, que são muito mais estáveis do que o tempo de parede.

## Baseline

| config | total (s) | M2 | M3 | O2 | O3 | corridas (total s) |
|---|---|---|---|---|---|---|
| baseline | 1398 | 161 | 101 | 337 | 295 | 970, 1398, 1871 |

Plano: sem cache, cada operação volta a ler e limpar os 12 ficheiros (O2 lê-os 4×, O3 3×). ~10 leituras por corrida.

## OPT-1 `materialize`: limpar uma vez e regravar com row groups pequenos

- **Observação.** Profiler do M3 (2019 jan-jun): stage de leitura com task máx 75 s vs mediana 2,7 s (skew 28×).
  `spark_partition_id()` mostrou que **todas** as linhas de cada ficheiro caem numa só partição: os ficheiros TLC têm
  **1 row group** cada (verificado em 2016-2020). O Spark parte cada ficheiro em 3-6 splits, mas só o split com o row
  group lê dados. Paralelismo real da leitura = nº de ficheiros. Somado às ~10 releituras+limpezas por corrida.
- **Hipótese.** Gravar as viagens limpas uma vez (`parquet.block.size` = 32 MB) e fazer M2/M3/O2/O3 ler daí elimina
  as releituras dos originais e dá várias tasks por ficheiro.
- **Mudança.** `--opt materialize` em `m1_clean.py` (`output/m1_trips`). O custo da escrita fica contado na M1.
- **Resultado.**

  | | baseline | materialize | variação |
  |---|---|---|---|
  | total (s) | 1398 | 661 | **−53 %** |
  | M1 (s) | 86 | 303 | +217 s (custo da escrita) |
  | M2 (s) / CPU (s) | 161 / 349 | 49 / 114 | −70 % / −67 % |
  | O2 (s) / tasks | 337 / 840 | 125 / 242 | −63 % |
  | O3 (s) / tasks | 295 / 721 | 116 / 164 | −61 % |

- **Conclusão.** O maior ganho do projeto. Paga-se uma escrita (~+217 s na M1) e cada operação seguinte fica 3× mais
  rápida. Compensa a partir de ~2 operações a jusante.
- **Correção verificada** (`scripts/compare.py`, `exceptAll` nos dois sentidos): M1, M3, O2 (flows e top20) e O3 iguais
  linha a linha à baseline, também com `agg_first`. Na M2, 1-2 das 993 linhas diferem só em ~1e-14 nas médias: a soma de
  doubles depende da ordem em que as partições são somadas (diferente com outra divisão dos ficheiros). Iguais na prática.

## OPT-2 `agg_first` na O2: agregar antes dos joins à lookup

- **Observação.** Na baseline da O2 os dois `BroadcastHashJoin` à tabela de zonas processam todas as viagens, e o
  shuffle do `groupBy` leva 4 colunas de texto (nomes e boroughs) por linha.
- **Hipótese.** Contar por (PU, DO, ano, mês) primeiro (≤ 263² rotas/mês) e só depois juntar os nomes reduz o trabalho
  dos joins e a largura das linhas no shuffle. O Catalyst não faz isto sozinho.
- **Mudança.** `--opt agg_first` em `o2_flows.py`. Testado: resultado igual à baseline (flows e top20).
- **Resultado** (medido sobre `materialize`, para isolar o efeito da O2 do ruído da leitura):

  | O2 | materialize | materialize + agg_first | variação |
  |---|---|---|---|
  | tempo (s) | 125 | 63 | **−50 %** |
  | CPU executores (s) | 360 | 132 | −63 % |
  | shuffle write (MB) | 74,8 | 39,0 | −48 % |
  | total do pipeline (s) | 661 | 567 | −14 % |

- Sem `materialize`, `agg_first` deu 2038 e 2032 s (2 corridas), pior do que a baseline. Mas a O3, que a `agg_first` não toca, também subiu de 295 para 649 s. Ou seja, nessas corridas a máquina estava mais lenta no geral, e estes números não servem para avaliar a `agg_first`.

## OPT-3 `cache`: manter as viagens limpas em memória (rejeitada)

- **Observação.** A mesma da OPT-1: cada operação volta a ler e limpar os originais.
- **Hipótese.** `trips.persist(MEMORY_AND_DISK)` evita as releituras sem pagar uma escrita em disco como o `materialize`.
- **Mudança.** `--opt cache` em `m1_clean.py`.
- **Resultado** (1 corrida, 2019): **falhou.** A M2 (primeira a usar a cache, e a que a preenche) demorou 789 s
  (baseline 161 s). Na M3 os executores morreram em série com `java.lang.OutOfMemoryError: Java heap space`
  (exit code 52, 8 executores perdidos) e o job abortou.
- **Porquê.** Cada executor tem 1200 MB de heap e a memória de execução e de armazenamento são partilhadas. As viagens
  limpas de 2019 (84 M linhas, ~1,5 GB em Parquet comprimido) ocupam várias vezes isso descomprimidas em memória, por
  isso a cache disputa o heap com as agregações e os joins. O `MEMORY_AND_DISK` manda para disco os blocos que não
  cabem, mas não protege a memória de execução das tasks. Com 4,5 GB de executores no total, a cache não serve. O
  `materialize` dá o mesmo benefício (ler uma vez limpo) sem ocupar o heap.
- O teste pequeno (1 mês) também foi mais lento do que a baseline: M2 200 s contra 161 s, O2 257 s contra 132 s.

## Medição intercalada (OPT-4 e OPT-5)

Os totais variavam muito de hora para hora: a M2, sem nenhuma mudança, deu 23 s numa série e 58 s noutra. Por isso
a OPT-4 e a OPT-5 foram medidas **intercaladas** (A, B, C, D, A, B, C, D, …, 3 rondas, `OPS=O2,O3`) e todas as
configurações apanharam as mesmas condições. A M1 é igual nas 4 e deu ~180 s em A, B e C, o que confirma que as
condições foram parecidas.

| config | O2 (s) | O2 CPU (s) | O2 tasks | O3 (s) | O3 CPU (s) | O3 tasks | O2+O3 (s) |
|---|---|---|---|---|---|---|---|
| A `materialize,agg_first` | 57 | 102 | 262 | 60 | 188 | 164 | 117 |
| B A + `cache_agg` | 113 | 143 | 2811 | 50 | 112 | 2485 | 163 |
| C B + `shuffle.partitions=24` | **40** | **60** | 560 | **36** | **93** | 313 | **76** |
| D A + `shuffle.partitions=24` | 64 | 123 | 219 | 44 | 139 | 174 | 108 |

Leituras das viagens (input records): A e D 188,8 M (O2) / 161,6 M (O3); B e C 80,8 M em ambas (1 leitura).

## OPT-4 `cache_agg`: guardar em cache só os intermédios pequenos

- **Observação.** Mesmo com OPT-1+2, o plano da O2 lê as viagens 4× e o da O3 3× (input records 2,3× e 2× o
  dataset). O `flows` (O2) e as contagens por hora `c` (O3) são reutilizados mas recalculados em cada uso.
- **Hipótese.** Guardá-los em cache (~330 k e ~1 M linhas, cabem na memória ao contrário da OPT-3) baixa cada
  operação para 1 leitura.
- **Mudança.** `--opt cache_agg`: `flows.cache()` em `o2_flows.py`, `c.cache()` em `o3_forecast.py`.
- **Resultado.** As leituras baixaram como previsto (O2 188,8 → 80,8 M registos), mas a **O2 ficou 2× mais lenta**
  (57 → 113 s) e a O3 só ganhou 10 s. O número de tasks subiu de 262 para 2811.
- **Diagnóstico** (plano `ab_B-2019-O2.txt`). No plano guardado em cache, o último shuffle (o da window da quota)
  fica em `hashpartitioning(..., 200)` **sem** `AQEShuffleRead coalesced`, ao contrário dos outros shuffles. A AQE
  preserva as partições de um plano guardado em cache. Assim, a cache fica com 200 partições minúsculas e cada uma
  das 4 leituras dela gera 200 tasks, cada uma com o seu custo fixo de arranque. Leva à OPT-5.

## OPT-5 `spark.sql.shuffle.partitions=24` (com a OPT-4)

- **Observação.** O que a OPT-4 deixou: ~2800 tasks na O2, por causa das 200 partições fixas da cache.
- **Hipótese.** O valor por omissão (200) é para clusters grandes. Temos 6 cores e os dados agregados têm poucos MB.
  Com 24 partições (4 por core) a cache fica em partições de tamanho útil. A AQE continua ligada e junta o resto.
  Segue a dica do professor no Piazza (a AQE "often struggles", ajudar com `shuffle.partitions`).
- **Mudança.** `CONF="spark.sql.shuffle.partitions=24"` no `spark-submit`, sem mudar o código.
- **Resultado** (C contra A): O2 57 → 40 s (CPU 102 → 60 s), O3 60 → 36 s (CPU 188 → 93 s), **O2+O3 117 → 76 s
  (−35 %)**. Tasks: O2 2811 → 560.
- **Controlo (D).** As 24 partições sem cache quase não mudam nada (O2+O3 108 s contra 117 s, dentro do ruído): sem
  cache, a AQE já juntava as 200 partições sozinha. **O ganho vem da combinação das duas:** a cache tira as releituras
  e as 24 partições tiram o custo que a cache trazia.
- **Correção.** `scripts/compare.py ab_C`: O2 (flows, top20) e O3 iguais à baseline.

## Não medido

- Pipeline completo 2016-2020: estimativa de mais de 1 h por corrida e ~8 GB de disco para o `materialize`. O Mac
  tinha 19 GB livres e pouca memória.

## Escalabilidade (configuração final OPT-1,2,4,5)

`scripts/scaling.sh`: rondas intercaladas com 3, 2 e 1 workers (2019 completo), e depois 10 % e 20 % com 3 workers.
A 3.ª ronda foi parada quando o Mac ficou sem memória, por isso w1 e as frações têm 2 corridas e w3/w2 têm 3.

| | total (s) | M1 (s) | paralelismo efetivo M1 | run time M1 (s) | CPU M1 (s) |
|---|---|---|---|---|---|
| 1 worker | 344 | 211 | 1,8 | 387 | 273 |
| 2 workers | 313 | 174 | 3,5 | 593 | 374 |
| 3 workers | 286 | 164 | 4,9 | 822 | 444 |
| 10 % (3 w) | 187 | 108 | | | |
| 20 % (3 w) | 161 | 87 | | | |

- Speedup com 3 workers: 1,20 (eficiência 40 %); com 2: 1,10 (55 %). O Spark usa os slots (o paralelismo sobe), mas
  o tempo total dos executores sobe na mesma proporção: os 3 "nós" partilham 4 cores físicos, um SSD e a pasta
  partilhada do Docker. Mais workers dão mais slots, não mais hardware.
- 10× mais dados custam só 1,5× mais tempo. O `--fraction` amostra depois de ler, por isso a M1 lê sempre tudo. Cada
  operação tem custos fixos (20–46 stages, 300–500 tasks), e as saídas são agregados limitados pelo nº de zonas.

## Medições finais no PC do Hugo (Apple M4) — números do relatório

`scripts/bench_all.sh` (fases info, main, cache, ab, scaling, check), resultados em `results-hugo/`. Apple M4 (10 cores),
16 GB, Docker VM 10 CPUs / 8,2 GB, mesma topologia (3 workers × 2 cores × 1500 MB), sem outros contentores.
Todas as saídas iguais à baseline (`results-hugo/compare.txt`; M2 com a diferença de ~1e-14 já conhecida).

| | baseline | OPT-1 | OPT-1+2 |
|---|---|---|---|
| total (s) | 139 | 110 (−21 %) | 99 (−29 %) |
| M1 / M2 / M3 / O2 / O3 (s) | 16 / 15 / 16 / 46 / 40 | 54 / 10 / 9 / 21 / 16 | 56 / 9 / 8 / 10 / 14 |

- OPT-1: M2–O3 117 → 56 s (−52 %), mas a escrita de 1,5 GB na pasta partilhada (bind mount) põe a M1 de 16 para 54 s.
- OPT-3 (`cache`): OutOfMemoryError no stage 17, como no portátil.
- Intercaladas (O2+O3): A 30 s, B 45 s (+50 %, 2983 tasks), C 25 s (−17 %; O3 16 → 11 s), D 27 s.
- Configuração final em corridas completas: 91, 173, 248 s. A M1 (só escrita/leitura na pasta partilhada) é a fonte do
  ruído (56 vs 126 s com o mesmo código na mesma ronda); o ganho de ~5 s da OPT-4+5 fica abaixo do ruído.
- Escalabilidade: 1/2/3 workers = 109/115/131 s (speedup 0,95 e 0,83). M1 fica em 68–70 s com paralelismo 1,8 → 5,0;
  CPU de M2–O3 sobe de 55 para 175 s para o mesmo trabalho (shuffles entre contentores, coordenação).
- Dados: 10 % / 20 % / 100 % = 62 / 86 / 131 s (10× dados → 2,1× tempo).
- Comparação: no Intel o gargalo era o trabalho de CPU (ganho −80 %); no M4 é o movimento de dados pela pasta partilhada.
