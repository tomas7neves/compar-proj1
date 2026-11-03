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

## Não medido

- Pipeline completo 2016-2020: estimativa de mais de 1 h por corrida e ~8 GB de disco para o `materialize`. O Mac
  tinha 19 GB livres e pouca memória.
