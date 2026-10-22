# Notas de estudo - Tomás (72901)

Uma secção por passo. Serve para o teste prático e para a discussão oral.

## Passo 2-3 - Dados e ambiente
O que fiz:      60 ficheiros 2016-2020 (6,4 GB, 456.751.055 viagens); cluster docker 3 workers x 2 cores x 3 GB, Spark 4.2.0
Porquê assim:   mesmo modelo do Lab 03 (driver no spark-master); 3x2 = 6 tasks em paralelo -> >= 12-18 partições por stage
Teoria:         Lec04 p.80 (2-3 tasks por core, partições 100-200 MB); Lec03 p.75 (driver / cluster manager / executores)
Descobertas:    passenger_count e RatecodeID: int64 até 2018-06, double depois -> ler ficheiro a ficheiro + cast (Passo 6)
Dúvidas:

## Passo 6 - M1
O que fiz:      TYPES (11 colunas + tipo final), months("1-12"), load() ficheiro a ficheiro + cast + lit(year/month)
                + reduce(unionByName), rules() com 5 critérios do enunciado + null_critical, stats numa só agregação.
Porquê assim:
  - Ler tudo com um spark.read.parquet falha: passenger_count/RatecodeID mudam de int64 para double em 2018-07.
  - Só 11 colunas -> column pruning (o Parquet é colunar, o FileScan só lê essas: ReadSchema no explain).
  - int em vez de bigint/double: códigos e contagens; menos bytes em memória e no shuffle.
  - year/month vêm do NOME do ficheiro (há timestamps errados, ex. 2008/2041, dentro de ficheiros de 2019).
  - unionByName e não union: union junta por posição e trocaria colunas sem dar erro.
  - Regras com F.coalesce(regra, False): null <= 0 dá null, e filter(~null) remove a linha SEM a contar.
    Por isso null_critical é uma regra explícita (nulls nas colunas usadas nas contas; RatecodeID/payment_type fora).
  - Contar as 6 regras com UMA agregação groupBy(year).agg(sum(regra.cast(int))...) = 1 leitura;
    6x filter().count() = 6 ações = 6 leituras. (antes/depois mensurável para o relatório)
  - stats.coalesce(1): 1 ficheiro; aceitável porque tem 1 linha por ano. coalesce é narrow (repartition faria shuffle).
  - trips sem cache = baseline; cada operação seguinte relê e relimpa tudo -> medir no Passo 11.
Teoria:         lazy evaluation (Lec03 p.76); narrow: select/cast/union/filter no mesmo stage (Lec04 p.19-23);
                partial aggregation = combiner (Lec04 p.42-45); column pruning / predicate pushdown (Catalyst).

Resultados (notebook, 2017-01 + 2019-01, 22 out):
  - Schema após load: 11 colunas com os tipos de TYPES + year/month (nullable=false, por serem F.lit).
  - Partições: 12 = 6 por ficheiro. maxSplit = min(128 MB, max(4 MB, bytes/cores)) = ~110 MB/6 cores ~ 18 MB
    -> o Spark parte o ficheiro para dar trabalho aos 6 cores (resposta à Q6.6).
  - Contagens por regra (uma viagem pode cair em várias):

    | regra             |      2017 |      2019 |
    |-------------------|----------:|----------:|
    | total             | 9.710.820 | 7.696.617 |
    | null_critical     |         0 |    28.672 |
    | fare_le_0         |     6.696 |     9.770 |
    | dist_le_0         |    58.678 |    55.089 |
    | pax_0_or_gt6      |       605 |   117.438 |
    | dropoff_le_pickup |     9.343 |     6.557 |
    | loc_out_of_range  |   197.915 |   187.558 |

  - Leitura: loc_out_of_range é a regra que mais remove (~2%, zonas 264/265 "Unknown").
    pax_0 explode em 2019 (605 -> 117k: passenger_count=0 passa a ser comum). Nulls só aparecem em 2019,
    coerente com a mudança para double em 2018-07. Sem null_critical, 28.672 linhas desapareciam sem contagem.
  - 2019-01 total = 7.696.617 = smoke test do Passo 3 (verificação OK).
  - explain() de stats (de baixo para cima): 2x FileScan parquet -> Project (casts + year literal) -> Union
    -> HashAggregate(partial_count/partial_sum) -> Exchange hashpartitioning(year, 200) -> HashAggregate(count/sum).
    1 Exchange = 1 shuffle -> previsão 2 stages. HashAggregate 2x = agregação parcial (combiner): cada partição
    envia 1 linha por ano em vez de milhões (Q6.3, Lec04 p.42-45).
  - O Catalyst otimizou sozinho:
      * lê 9 colunas, não 11: RatecodeID/payment_type/month não são usados nas stats -> não lidos (column pruning
        atravessa o plano todo, não só o select);
      * cast de double -> double removido (trip_distance, fare_amount...);
      * coalesce(isnull(...), false) removido (isnull nunca é null);
      * ~between(1,263) reescrito como (x < 1) OR (x > 263).
  - AdaptiveSparkPlan isFinalPlan=false: plano antes de correr (AQE ativo).
    hashpartitioning(year, 200) = 200 partições de shuffle para 2 chaves -> 198 vazias; AQE junta-as.
    Candidato a otimização: spark.sql.shuffle.partitions ajustado ao tamanho do problema.

  - Bloco 4 (stats + trips), 2017-01 + 2019-01:
      removed_any 2019 = 369.508 (soma das regras 405.084 -> ~35k viagens falham >1 regra); removed_any 2017 = 241.596
      limpas = 17.407.437 - 611.104 = 16.796.333 (bate certo com trips.count())
  - Spark UI (Jobs), mapeado pela hora de submissão:
      job 0,1   read.parquet  1 task cada, lê só o rodapé do Parquet (schema) -> load() não é 100% grátis
      job 2+3   groupBy(year).count().show()        40 s (1ª leitura, cache fria)
      job 4+5   contagem por regra                  38 s
      job 6+7   stats.coalesce(1).write.json        job 6: stage 1, 12 tasks, 14 s | job 7: stage 2, 1 task, 1 s (12 skipped)
      job 8+9   trips.count()                        23 s -> relê os 2 ficheiros do zero (sem cache)
      job 10    spark.read.json sem schema -> job só para inferir o schema
      job 11    show
  - Interpretação:
      * AQE parte um job com shuffle em 2 jobs (stage de map, depois stage final) e perde a descrição
        ($anonfun$withThreadLocalCaptured). "12 skipped" = shuffle files já existiam do job anterior, não repete.
      * Stage final com 1 task e não 200: AQE coalesceu as partições do shuffle (só 2 linhas).
      * Mesma leitura 40 s (14:26) vs 14 s (15:17): cache do SO + JIT -> descartar 1ª corrida, usar mediana.
      * trips sem cache: cada ação refaz toda a linhagem (ler + cast + filtrar) -> custo x nº de operações.

Previsão vs real:
  - Previsão: 1 Exchange -> 2 stages. Real: 2 stages (em 2 jobs por causa do AQE). Stage final 1 task (AQE).
Dúvidas:
Perguntas que me podem fazer no oral:
  - Porque é que load() é instantâneo e o .show() demora? (transformações vs ações)
  - De onde vêm as 12 partições?
  - O que acontece a uma linha com fare_amount null sem o coalesce? E nas estatísticas?
  - Porque unionByName e não union? Porque não ler os 60 ficheiros de uma vez?
  - Quantos stages tem o job das stats e porquê? (shuffles + 1)
