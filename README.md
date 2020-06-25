# desafio-nasa
Desafio NASA Semantix

Intro
------
Prezado candidato

Gostaríamos de fazer um teste que será usado para sabermos a sua proficiência nas habilidades para a vaga. O teste
consiste em algumas perguntas e exercícios práticos sobre Spark e as respostas e códigos implementados devem ser
armazenados no GitHub. O link do seu repositório deve ser compartilhado conosco ao final do teste.
Quando usar alguma referência ou biblioteca externa, informe no arquivo README do seu projeto. Se tiver alguma
dúvida, use o bom senso e se precisar deixe isso registrado na documentação do projeto

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

Programa
------

