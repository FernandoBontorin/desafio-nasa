# desafio-nasa
Desafio NASA

Perguntas
------

#### Qual o objetivo do comando cache em Spark?
> O comando 'cache' informa a api do Spark que até aquele ponto de execução deve se por os dados em alguma memoria de rapido acesso. Pode se determinar por parametro o tipo de memoria (principal ou secundaria) e o estado dos dados (serializados ou não)

#### O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?
> O Spark resolve as etapas da execução e prepara um plano de execução antes de executar as etapas. Esse preparo as vezes elimina processos redundantes nas etapas e gera algumas otimizações

#### Qual é a função do **SparkContext**?
> O SparkContext é a API do spark que permite usar mapReduce de forma distrubuida. Gerencia operações IO, Sessão e acesso aos RDDs

#### Explique com suas palavras o que é **Resilient​ ​Distributed​ ​Datasets​ (RDD)**.
> Conjunto de dados distribuidos com tolerancia a falhas

#### **GroupByKey** é menos eficiente que **reduceByKey** em grandes dataset. Por quê?
> O reduce diferentemente do group, não executa operações de distribuição das informações, cada operação de reduce é executada localmente e apenas o resultado é compartilhado (assim reduz IO)


Explique o que o código Scala abaixo faz.
```scala
val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...")
```
> Conta a quantidade de palavras por palavra em um arquivo e salva o resultado.

Valores
------
1. Número de hosts únicos.
> 75060

2. O total de erros 404.
> 20112

3. Os 5 URLs que mais causaram erro 404.
> "dialip-217.den.mmc.com",
    "piweba3y.prodigy.com",
    "155.148.25.4",
    "maz3.maz.net",
    "gate.barr.com"

4. Quantidade de erros 404 por dia.
>  ("1995-08-08", 792),
   ("1995-08-31", 1102),
   ("1995-08-01", 486),
   ("1995-08-18", 514),
   ("1995-08-21", 620),
   ("1995-08-16", 506),
   ("1995-08-11", 544),
   ("1995-08-17", 552),
   ("1995-08-26", 716),
   ("1995-08-24", 832),
   ("1995-08-07", 1048),
   ("1995-08-23", 718),
   ("1995-08-22", 532),
   ("1995-08-25", 838),
   ("1995-08-09", 568),
   ("1995-08-04", 698),
   ("1995-08-30", 1078),
   ("1995-08-20", 612),
   ("1995-08-13", 440),
   ("1995-08-29", 832),
   ("1995-08-06", 758),
   ("1995-08-15", 648),
   ("1995-08-03", 576),
   ("1995-08-10", 624),
   ("1995-08-14", 594),
   ("1995-08-19", 426),
   ("1995-09-01", 52L,
   ("1995-08-05", 474),
   ("1995-08-27", 768),
   ("1995-08-28", 812),
   ("1995-08-12", 352)

5. O total de bytes retornados
> 53656682848

Programa
------
#### Get Started
Just run application by type spark-submit and pass files you want to get 
```bash
sbt clean package
spark-submit --class nasa.NasaJob target\scala-2.11\desafio-nasa_2.11-0.1.0.jar [hdfs://, s3:// ...]
```


