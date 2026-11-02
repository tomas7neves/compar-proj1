# Respostas do Hugo (M3, O2, O3)

> **Estado:** as respostas conceptuais (secções 2–4 e 6) são um **rascunho para o Hugo rever** antes de irem para o relatório ou para o oral.
> Os números das secções 1 e 5 **não foram corridos pelo Hugo**: vêm da auditoria do Tomás (`spark-master`, `local[3]`, 2019 jan–set, com `fraction < 1`). Alguns valores chegaram cortados na mensagem e ficam assinalados com "(cortado)".

---

## 1. Execução dos notebooks

**a) Não corridos pelo Hugo no Jupyter**, e por isso não têm outputs. O código das células foi testado só com dados sintéticos (`spark-submit` num contentor `spark:4.2.0-python3`, em `local[2]`, sem cluster e sem dataset real), em casos com resposta conhecida:

| Notebook | Casos testados |
|---|---|
| M3 | zona com um mês sem viagens (grelha), salto de 60 para 200 viagens (anomalia), zona com 3 viagens/mês (pouco volume) |
| O2 | rota cuja quota sobe em Q1 e desce em Q3 (inversão), rota sem viagens num mês |
| O3 | pico numa segunda, horas sem viagens (média e variância com zeros), lag de 28 dias, timestamp de 2008 |

**b) Números da auditoria do Tomás** (as funções `run()` extraídas dos notebooks e o pipeline completo):

- **M3:**
  - 2367 linhas.
  - As quotas somam 1 (mínimo 0,9999999999999994, máximo 1,0000000000000004).
  - 642 anomalias, das quais 37 com `rolling_std = 0`.
  - Top de Manhattan em maio: UES South 16 996, UES North 15 196, Midtown Center 14 848.
- **O2:**
  - O `top20` tem 110 linhas (EWR só tem 10 rotas).
  - 10 dessas linhas tinham as quotas a `null`. Era o bug 1, agora corrigido (secção 3).
- **O3:**
  - 388 986 linhas e 76 809 com `high_demand`.
  - 0 variâncias negativas.
  - `high_demand` por `service_zone`: Boro Zone 61 %, Yellow 7 %, Airports 4 %.
- **Testes sintéticos do Tomás:**
  - M3: grelha com meses a 0, janela a atravessar o ano, `rank`, quota e anomalia;
  - O2: quota e inversão;
  - O3: média e variância com zeros, lag e o timestamp de 2008.

**c) Os `explain()` das leituras repetidas.** Os dados de teste são Parquet e CSV falsos, com o schema real (9 meses de 2019, 30 mil viagens por mês, 20 zonas), e passam pelo M1 verdadeiro:
- scripts em `scripts/sintetico/gerar_dados.py` e `scripts/sintetico/planos.py`;
- planos em `results/plans/sintetico-{M3,O2,O3}.txt`.

Os planos batem com os da auditoria:

| | M3 | O2 (escrita `top20`) | O3 |
|---|---|---|---|
| Exchange | 7 | 11 | (o real tem 6, ver Q9.5) |
| Leituras completas das viagens | 1 | 3 na auditoria (27 FileScan = 3 × 9), +1 da escrita `flows` = **4** | 3 |

**Diferença no O2.** Nos meus dados sintéticos (`fraction = 1`) o plano tinha 22 FileScan e não 27. O ramo do pivot só lia jan, mar, jul e set, porque o `filter(month IN (1,3,7,9))` elimina os ramos do `union` do M1 em que `month` é outro `lit`. Com `fraction < 1`, como na auditoria, o `sample` fica entre o `union` e o filtro e impede esse corte: são 3 leituras completas. As duas contagens estão certas, cada uma nas suas condições, e o texto do notebook explica agora os dois casos.

---

## 2. M3 (rascunho)

**Q8.1 – `groupBy().agg(avg)` vs `avg().over(Window)`.**
- O `groupBy` **colapsa** cada grupo numa linha.
- A window **mantém todas as linhas** e escreve em cada uma o valor do seu grupo. Também aceita ordem e moldura (`rangeBetween`), o que permite médias móveis.

Os dois fazem shuffle pelas chaves. No M3 usam-se os dois: o `groupBy` (zona, mês) reduz os milhões de viagens a cerca de 2,4 mil linhas, e as windows trabalham sobre essas linhas.

**Q8.2 – Os 7 Exchange, o SMJ, o BHJ e o cross join** (`results/plans/sintetico-M3.txt` tem os mesmos 7 Exchange, 1 SMJ, 1 BHJ e 1 BroadcastNestedLoopJoin da auditoria):

| # | Exchange (hashpartitioning) | Porquê |
|---|---|---|
| 1 | `borough, PULocationID, zone, year, month` | `groupBy` por (zona, mês) |
| 2 | `PULocationID, year, month` (lado `z`) | os dois lados do **SortMergeJoin** (left) entre a grelha e `z` |
| 3 | `PULocationID, year, month` (lado grelha) | |
| 4 | `borough, year, month` | window do `rank` |
| 5 | `PULocationID` | window `zone_3m` |
| 6 | `borough` | window `borough_3m` |
| 7 | `PULocationID` | window `zone_prev`. Repete o 5, porque a window `borough` está entre as duas |

- **BroadcastHashJoin:** viagens × zonas. O CSV tem 265 linhas, abaixo dos 10 MB.
- **Cross join:** zonas × meses. Não tem chave, e por isso o Spark faz broadcast da tabela pequena e executa-o como **BroadcastNestedLoopJoin**.
- **SortMergeJoin:** grelha × `z`. Os dois lados são saídas de agregações, e sem estatísticas o Spark estima-os como grandes. O AQE pode passá-lo a broadcast em execução.

Pôr a window `borough` antes das duas de `PULocationID` poupava o Exchange 7, mas `z` é pequeno e o ganho deve ser quase nulo.

**Q8.3 – Porquê `m_idx` contínuo?** O `rangeBetween(-2, 0)` mede distância **em valor**. Com `month` sozinho, janeiro (1) fica a 11 de dezembro (12) e a janela partia na mudança de ano. Com `year*12 + month`, dezembro de 2018 dá 24228 e janeiro de 2019 dá 24229.

**Q8.4 – Porquê `rangeBetween(-3, -1)`?** Se o mês atual entra na sua própria média, puxa a média e o σ para si.
- Com 3 valores, o pior caso é 0, 0 e x: média x/3 e σ (amostral) = x/√3.
- O desvio do x é 2x/3, o que dá **(2x/3)/(x/√3) = 2/√3 ≈ 1,155σ**.
- Nunca passa de 1,5σ, e o alerta **nunca disparava**.

Com os 3 meses **anteriores**, o mês é comparado com o histórico.

**Q8.5 – E se faltasse o `partitionBy`?** A janela passa a ser a tabela toda:
- o `rank` fica global, e não por borough;
- as somas a 3 meses misturam zonas.

Na execução, o Spark move **todas as linhas para uma partição** (aviso "No Partition Defined for Window operation"), o que dá 1 task e zero paralelismo.

**Porquê `rank`?** Zonas com o mesmo volume ficam na mesma posição (1, 2, 2, 4), como no `RANK() OVER` do slide.
- `row_number` desempatava arbitrariamente, e o resultado podia mudar entre execuções. Foi exatamente o bug 2 da O2.
- `dense_rank` também servia.

**Porquê `coalesce(..., False)` e não `null`?** Por coerência com o M1, que também passa nulls a `False`. Assim a flag é sempre booleana e `sum(cast int)` conta as anomalias diretamente. O "não avaliável" não se perde, porque fica em `rolling_std = null`. Se o guia pede `null`, basta tirar o `coalesce`.

**Zonas sem viagens na grelha (103/104/105, ilhas).** Entram com `trip_count = 0` em todos os meses: ficam empatadas no fim do ranking, com quota 0, e nunca são anomalia. **Para o ranking não é o ideal**, porque enchem o fim da lista com empates. Proposta: tirar as zonas com total 0 no período com `F.sum("trip_count").over(Window.partitionBy("PULocationID")) > 0`. Não lê as viagens outra vez e usa a partição das windows de zona. Ainda não mudei: fica para decidirmos.

---

## 3. O2 (rascunho)

**Q9.1 – Broadcast ou sort-merge? Colunas do shuffle?**
- Os 2 joins com as zonas são **BroadcastHashJoin**, porque o CSV está abaixo de `autoBroadcastJoinThreshold` (10 MB).
- O shuffle do `groupBy` leva **8 chaves**: `PULocationID`, `pu_zone`, `pu_borough`, `DOLocationID`, `do_zone`, `do_borough`, `year`, `month`. A estas soma-se o `count` parcial. **4 das chaves são strings.**

**Q9.2 – Porque é que `agg_first` reduz trabalho?** Agregar só por (`PULocationID`, `DOLocationID`, `year`, `month`) e juntar os nomes depois tem dois efeitos:
- os 2 joins correm sobre cerca de 69 mil rotas por mês, em vez de todas as viagens;
- o shuffle leva 4 inteiros em vez de 4 inteiros mais 4 strings.

É o princípio do `reduceByKey` (combinar antes do shuffle): reduzir cedo e enriquecer depois.

**Q9.3 – Quantas leituras e onde se veem.** São 4 leituras na auditoria (ver 1c). Onde se veem:
- **Profiler:** no `input_records` da operação O2 (`results/metrics/*.json`), que deve dar cerca de 4 × as viagens carregadas. O `input_bytes` não serve, como diz a nota no profiler.
- **Spark UI:** no separador SQL, há 2 queries com vários `Scan parquet`.

**Q9.4 – "Inverte entre Q1 e Q3".**
- **O que usei:** tendência = quota(último mês) − quota(primeiro mês) de cada trimestre. Há inversão quando os sinais das tendências de Q1 e de Q3 são opostos.
- **Alternativas consideradas:**
  1. Declive de regressão nos 3 meses. Com meses igualmente espaçados, o declive é (y₃ − y₁)/2, por isso tem o mesmo sinal: é equivalente.
  2. Média de Q1 contra média de Q3. Mede uma subida ou descida, não uma inversão.
  3. Mudanças de sinal mês a mês. Fica mais sensível ao ruído.
- **Porquê só o top-20:** o enunciado junta as duas coisas no mesmo ponto. Além disso, em rotas pequenas a quota oscila por acaso, e quase todas "inverteriam". A alternativa seria aplicar a todas as rotas com um volume mínimo.

**Correções feitas (branch `fix-o2`)**, no notebook e na `run()` final:

1. **Quotas `null` no top-20.** Uma rota sem viagens em jan, mar, jul e set desaparecia no `filter` antes do pivot. O `left join` deixava-a toda a `null`, e o `fillna` nunca a apanhava.
   - **Agora:** `top` × anos carregados (`crossJoin`), depois `left join` com o pivot em `[PULocationID, DOLocationID, year]`, e só então `fillna(0)` e as tendências.
2. **`row_number` sem desempate.** Com empates em `total_trips`, as duas avaliações de `top` (resultado e filtro do pivot) podiam escolher rotas diferentes.
   - **Agora:** `orderBy(desc("total_trips"), "PULocationID", "DOLocationID")`.
3. **(menor) Texto das leituras** atualizado com os dois casos (`fraction = 1` e `< 1`).
4. **(menor) `flows.unpersist()`** no fim do notebook não libertava nada, porque `flows` foi reatribuído no O2-3. Passou a `spark.catalog.clearCache()`.

**Teste mínimo (passa):**
- uma rota de Staten Island com viagens **só em fevereiro** dá `year = 2019`, as 4 quotas a 0 e `share_reversal = False`;
- nenhuma linha fica com quota ou ano a `null`;
- duas rotas de Manhattan empatadas (90 viagens cada) ficam sempre em 1º e 2º pela ordem dos IDs.

---

## 4. O3 (rascunho)

**Q9.5 – Porque é que o self-join é sort-merge?** Os dois lados são a tabela de contagens (zona × dia × hora), com centenas de milhares de linhas por trimestre. Sem estatísticas, o Catalyst estima uma agregação com o tamanho da entrada (os Parquet todos), que fica muito acima dos 10 MB, e por isso escolhe **SortMergeJoin**. A auditoria tem 1 SMJ e 3 BHJ (n_days, histórico e zonas).

**Atenção:** com os meus dados sintéticos (ficheiros minúsculos) o mesmo join saiu como **BroadcastHashJoin** (`sintetico-O3.txt`, com 4 BHJ e 4 Exchange contra os 6 do real). A estratégia de join depende do tamanho, e convém mostrar o plano real no relatório. Em execução, o AQE ainda o pode passar a broadcast.

**Q9.6 – Skew?** Não observei, porque não corri com dados reais. Onde procurar, com o `skew` (máximo/mediana) que o profiler grava por stage:
- no O3, os stages com `hashpartitioning(PULocationID, data, hora)`. São muitas chaves, por isso skew pouco provável;
- no M3, a window por `borough`: são 6 chaves em 200 partições e o Manhattan fica numa só task, mas com dados já agregados.

O AQE só faz skew join em SortMergeJoin quando uma partição é > 5× a mediana **e** > 256 MB (valores por defeito). Com estes volumes não deve disparar.

**Q9.7 – Horas sem viagens.** Os zeros não somam nada a Σx nem a Σx², só contam no N, que vem do calendário:
```
média = Σx / N        variância = (Σx² − N·média²) / (N − 1)
```
Exemplo: zona X, segundas às 4h, jan–mar de 2019 (N = 12 segundas). Houve 3 viagens numa segunda, 1 noutra e 0 nas outras 10.
- Σx = 4 e Σx² = 10, por isso média = 0,333.
- var = (10 − 12 × 0,333²)/11 = 0,788.
- Um `avg` sobre as linhas existentes dava (3 + 1)/2 = **2**, ou seja, 6× a média real.

As 0 variâncias negativas da auditoria confirmam que a fórmula não tem problemas numéricos.

**Porquê filtrar timestamps fora do mês do ficheiro?** A data é a chave do self-join e o N vem do calendário dos meses carregados. Uma viagem de 2008 no ficheiro de janeiro de 2019 criava um dia sem N, sem histórico e sem lag.

**Porquê `hist_mean` com o período todo?** É a leitura mais simples de "média histórica" e não precisa de mais uma window. Incluir o próprio dia dilui o pico em 1/N (cerca de 8 % com N = 12). Usar só dias anteriores obrigava a uma window por data e deixava os primeiros dias sem histórico.

**Lag `null` vs `0`.**
- `0` é uma observação: o dia de há 28 dias está nos dados e não teve viagens.
- `null` é "não sei": o dia está fora do período carregado.

Pôr 0 nas primeiras 4 semanas inventava quedas de procura.

**Porquê o self-join sobre contagens e não sobre viagens?** O lag é uma propriedade da contagem (zona, dia, hora). Um self-join viagem a viagem juntava cada viagem com **todas** as da mesma zona e hora de há 28 dias, um join muitos-para-muitos (n × m linhas por chave), e depois ainda era preciso contar. Não sabia que o guia previa essa baseline. Se a quiserem como "antes" no estudo de otimização, posso escrevê-la como variante.

---

## 5. Números para o relatório

**Não corridos pelo Hugo. Números da auditoria do Tomás** (`local[3]`, 2019 jan–set, `fraction < 1`). A baseline no cluster quem corre é o Tomás.

| Operação | Tempo | Stages |
|---|---|---|
| M1 | 73 s | |
| M2 | 31 s | |
| M3 | 86 s | 26 |
| O2 | (cortado) | |
| O3 | 52 s | 17 |
| **Total** | **330 s** | |

| Plano | FileScan | Exchange | Outros |
|---|---|---|---|
| M3 | 9 | 7 | 1 BHJ, 1 SMJ, 1 cross join (BNLJ) |
| O2 | 27 | 11 | 6 BHJ, 2 SMJ; 3 leituras, +1 na escrita de `flows` = 4 |
| O3 | 27 | 6 | 3 BHJ, 1 SMJ; 3 leituras |

O FileScan conta 9 ficheiros por cada leitura das viagens.

---

## 6. Próximos passos (rascunho)

| Otimização | Onde | Hipótese (confirmar com antes/depois) |
|---|---|---|
| `cache` de `flows` / `c` | O2, O3 | `input_records` passa de 4× para 1× (O2) e de 3× para 1× (O3); o tempo desce perto da fração gasta em leitura e join. Risco: spill com executores de 1500 MB |
| `cache` de `trips` entre operações | todas | hoje cada operação relê e limpa os Parquet (N operações → N+1 leituras) |
| `agg_first` | O2 | shuffle do `groupBy` menor (sem as 4 strings) e os 2 joins sobre cerca de 69 mil linhas por mês |
| `spark.sql.shuffle.partitions` | M3, O2, O3 | 200 partições para tabelas de milhares de linhas são sobretudo overhead (o M3 tem 26 stages). Com cerca de 2–3 × cores, menos tasks e menos agendamento. Comparar com o AQE, que já junta partições |
| reordenar windows | M3 | −1 Exchange. Ganho esperado quase nulo, porque `z` é pequeno: bom exemplo de "não vale a pena" |
| skew / AQE | O3, M3 | max/mediana < 3 nos stages grandes; a window por `borough` concentra o Manhattan, mas sobre dados agregados |

- **Q5.1–Q5.4 (`main.py` e profiler):** ainda não tenho as perguntas. Manda-mas.
- **Falta no repo:** nada meu além deste commit. Não tenho `scripts/run.sh` nem gráficos; os únicos scripts são os de `scripts/sintetico/`.
