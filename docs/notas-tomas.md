# Notas de estudo - Tomás (72901)

Uma secção por passo. Serve para o teste prático e para a discussão oral.

## Passo 2-3 - Dados e ambiente
O que fiz:      60 ficheiros 2016-2020 (6,4 GB, 456.751.055 viagens); cluster docker 3 workers x 2 cores x 3 GB, Spark 4.2.0
Porquê assim:   mesmo modelo do Lab 03 (driver no spark-master); 3x2 = 6 tasks em paralelo -> >= 12-18 partições por stage
Teoria:         Lec04 p.80 (2-3 tasks por core, partições 100-200 MB); Lec03 p.75 (driver / cluster manager / executores)
Descobertas:    passenger_count e RatecodeID: int64 até 2018-06, double depois -> ler ficheiro a ficheiro + cast (Passo 6)
Dúvidas:

## Passo 6 - M1
O que fiz:
Porquê assim:
Teoria:
Previsão vs real:
Dúvidas:
Perguntas que me podem fazer no oral:
