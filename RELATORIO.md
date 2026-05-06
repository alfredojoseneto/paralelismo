# Relatório de Execução — Trabalho 02: Paralelismo Parte C

- DIRETORIA DE EDUCAÇÃO CONTINUADA - IEC
- Disciplina: ARMAZENAMENTO E PROCESSAMENTO MASSIVO E DISTRIBUÍDO DE DADOS
- Nome do curso: ENGENHARIA DE DADOS - O8 - T1
- Professor: Ricardo Brito Alves
- Trabalho: Sistemas Distribuídos
- Grupo:
    - Alfredo José Alves Rodrigues Neto
    - Carlos Eduardo Fernandes Souza
    - Cláudio Henrique de Faria Pontes
    - Dgeison Serrão Peixoto
    - Eduardo Luis Hosda
    - Francisco Duclou Rito
- Paradigma: Divisão e Conquista com `multiprocessing`
- Dataset: `yellow_tripdata_2016-02.csv` — Corridas de táxi amarelo de Nova York (fevereiro/2016)

---

## Sumário

- [Relatório de Execução — Trabalho 02: Paralelismo Parte C](#relatório-de-execução--trabalho-02-paralelismo-parte-c)
  - [Sumário](#sumário)
  - [1. Ambiente de execução](#1-ambiente-de-execução)
  - [2. Etapa 1 — Dividir](#2-etapa-1--dividir)
  - [3. Etapa 2 — Conquistar](#3-etapa-2--conquistar)
  - [4. Etapa 3 — Combinar](#4-etapa-3--combinar)
  - [5. Etapa 4 — Resultados da execução](#5-etapa-4--resultados-da-execução)
    - [5.1 Versão Sequencial (baseline)](#51-versão-sequencial-baseline)
    - [5.2 Versão Paralela — 2 workers](#52-versão-paralela--2-workers)
    - [5.3 Versão Paralela — 4 workers](#53-versão-paralela--4-workers)
    - [5.4 Versão Paralela — 8 workers (oversubscription)](#54-versão-paralela--8-workers-oversubscription)
    - [5.5 Versão Paralela — 10 workers (oversubscription)](#55-versão-paralela--10-workers-oversubscription)
    - [5.6 Versão Paralela — 20 workers (oversubscription)](#56-versão-paralela--20-workers-oversubscription)
  - [6. Etapa 5 — Tabela comparativa](#6-etapa-5--tabela-comparativa)
  - [7. Discussão](#7-discussão)
    - [7.1 A versão paralela foi mais rápida?](#71-a-versão-paralela-foi-mais-rápida)
    - [7.2 O tamanho do chunk influenciou?](#72-o-tamanho-do-chunk-influenciou)
    - [7.3 Houve custo extra de criação de processos?](#73-houve-custo-extra-de-criação-de-processos)
    - [7.4 Em que cenário a paralelização compensa?](#74-em-que-cenário-a-paralelização-compensa)
  - [8. Conclusão](#8-conclusão)

---

## 1. Ambiente de execução

| Parâmetro                         | Valor                         |
| --------------------------------- | ----------------------------- |
| Arquivo processado                | `yellow_tripdata_2016-02.csv` |
| Tamanho do arquivo                | **1,78 GB**                   |
| Núcleos físicos disponíveis       | **4**                         |
| Configurações de workers testadas | `[2, 4, 8, 10, 20]`           |
| Python                            | 3.11                          |
| SO                                | Linux                         |

> Configurações com mais de **4 workers** caracterizam **oversubscription** — mais processos do que núcleos físicos disponíveis.

---

## 2. Etapa 1 — Dividir

A função `get_chunks()` particiona o arquivo em `n` blocos definidos por pares de **byte-offsets** `(start, end)`, sem carregar nenhum dado na memória neste momento.

O algoritmo:

1. Lê apenas a primeira linha para medir o tamanho do cabeçalho em bytes e excluí-lo dos blocos de dados.
2. Calcula o tamanho aproximado de cada bloco: `chunk_size = data_size // n_chunks`.
3. Para cada bloco (exceto o último), avança o ponteiro de leitura até o final da linha mais próxima, garantindo que **nenhuma linha seja cortada ao meio**.
4. O último bloco é sempre estendido até o fim absoluto do arquivo.

Com o arquivo de 1,78 GB, os tamanhos médios de chunk por configuração foram:

| Workers | Chunk médio |
| ------- | ----------- |
| 2       | 891,8 MB    |
| 4       | 445,9 MB    |
| 8       | 222,9 MB    |
| 10      | 178,4 MB    |
| 20      | 89,2 MB     |

---

## 3. Etapa 2 — Conquistar

A função `process_chunk()` é executada **em um processo separado** para cada bloco. Cada worker:

1. Abre o arquivo em modo binário e posiciona o ponteiro em `start` via `f.seek(start)`.
2. Usa um **gerador de linhas** (`line_gen`) que lê e decodifica somente as linhas dentro do intervalo `[start, end)`, sem carregar o bloco inteiro na memória.
3. Alimenta um `csv.reader` com esse gerador para fazer o parse correto dos campos.
4. Acumula as seguintes métricas em um dicionário `Stats` local ao processo:
   - contagem de viagens
   - número de passageiros
   - distância percorrida
   - tarifa base, gorjeta e total cobrado
   - contagem de erros de parsing

Como cada processo opera em seu próprio intervalo de bytes sem sobreposição, **não há necessidade de locks ou variáveis compartilhadas**, eliminando problemas de condição de corrida.

---

## 4. Etapa 3 — Combinar

A função `aggregate()` recebe a lista de `Stats` parciais retornada por todos os workers e aplica um **reduce** simples: soma campo a campo todos os dicionários em um único `Stats` global.

Este é o passo final do padrão **map-reduce**:

```
map    -> process_chunk()  (executado em paralelo por cada worker)
reduce -> aggregate()      (executado uma única vez no processo principal)
```

---

## 5. Etapa 4 — Resultados da execução

Todas as configurações processaram exatamente o mesmo conjunto de **11.382.049 viagens** sem nenhum erro de parsing, confirmando a corretude do particionamento e da agregação.

### 5.1 Versão Sequencial (baseline)

Processamento inteiro em um único processo, linha a linha, sem paralelismo.

| Métrica                  | Valor              |
| ------------------------ | ------------------ |
| Viagens processadas      | 11.382.049         |
| Erros de parsing         | 0                  |
| Distância média          | 5,0608 milhas      |
| Tarifa média             | US$ 12,4141        |
| Gorjeta média            | US$ 1,7724         |
| Receita total            | US$ 177.589.959,20 |
| Passageiros médio/viagem | 1,6552             |
| **Tempo de execução**    | **45,955 s**       |

---

### 5.2 Versão Paralela — 2 workers

Arquivo dividido em **2 blocos de ~891,8 MB** cada, processados simultaneamente.

| Métrica                  | Valor              |
| ------------------------ | ------------------ |
| Viagens processadas      | 11.382.049         |
| Erros de parsing         | 0                  |
| Distância média          | 5,0608 milhas      |
| Tarifa média             | US$ 12,4141        |
| Gorjeta média            | US$ 1,7724         |
| Receita total            | US$ 177.589.959,18 |
| Passageiros médio/viagem | 1,6552             |
| **Tempo de execução**    | **24,318 s**       |
| Speedup vs. sequencial   | **1,89x**          |
| Eficiência               | 94,5%              |

---

### 5.3 Versão Paralela — 4 workers

Arquivo dividido em **4 blocos de ~445,9 MB** cada — configuração que iguala o número de núcleos físicos da máquina.

| Métrica                  | Valor              |
| ------------------------ | ------------------ |
| Viagens processadas      | 11.382.049         |
| Erros de parsing         | 0                  |
| Distância média          | 5,0608 milhas      |
| Tarifa média             | US$ 12,4141        |
| Gorjeta média            | US$ 1,7724         |
| Receita total            | US$ 177.589.959,19 |
| Passageiros médio/viagem | 1,6552             |
| **Tempo de execução**    | **24,852 s**       |
| Speedup vs. sequencial   | **1,85x**          |
| Eficiência               | 46,2%              |

> Apesar de usar todos os 4 núcleos, o tempo foi ligeiramente maior que com 2 workers. Isso indica que o gargalo está no acesso ao disco (I/O-bound): 4 processos competem simultaneamente pelo mesmo arquivo, gerando contenção de I/O que anula parte do ganho de CPU.

---

### 5.4 Versão Paralela — 8 workers (oversubscription)

Arquivo dividido em **8 blocos de ~222,9 MB** cada. Com apenas 4 núcleos físicos, o sistema operacional passa a fazer **context-switch** entre os 8 processos.

| Métrica                | Valor        |
| ---------------------- | ------------ |
| Viagens processadas    | 11.382.049   |
| Erros de parsing       | 0            |
| **Tempo de execução**  | **23,984 s** |
| Speedup vs. sequencial | **1,92x**    |
| Eficiência             | 24,0%        |

> Overhead de oversubscription relativo ao melhor resultado: **+1,370 s**.

---

### 5.5 Versão Paralela — 10 workers (oversubscription)

Arquivo dividido em **10 blocos de ~178,4 MB** cada.

| Métrica                | Valor        |
| ---------------------- | ------------ |
| Viagens processadas    | 11.382.049   |
| Erros de parsing       | 0            |
| **Tempo de execução**  | **23,953 s** |
| Speedup vs. sequencial | **1,92x**    |
| Eficiência             | 19,2%        |

> Overhead de oversubscription relativo ao melhor resultado: **+1,339 s**.

---

### 5.6 Versão Paralela — 20 workers (oversubscription)

Arquivo dividido em **20 blocos de ~89,2 MB** cada. Configuração com maior grau de oversubscription (5x os núcleos físicos).

| Métrica                | Valor        |
| ---------------------- | ------------ |
| Viagens processadas    | 11.382.049   |
| Erros de parsing       | 0            |
| **Tempo de execução**  | **22,614 s** |
| Speedup vs. sequencial | **2,03x**    |
| Eficiência             | 10,2%        |

> Paradoxalmente, esta configuração apresentou o **melhor tempo absoluto**, sugerindo que blocos menores permitiram melhor aproveitamento do cache de disco (page cache do kernel), compensando o overhead de context-switch.

---

## 6. Etapa 5 — Tabela comparativa

| Configuração             | Chunk médio | Tempo (s) | Speedup | Eficiência |
| ------------------------ | ----------- | --------- | ------- | ---------- |
| Sequencial (1 processo)  | —           | 45,955    | 1,00x   | 100,0%     |
| Paralela (2 workers)     | 891,8 MB    | 24,318    | 1,89x   | 94,5%      |
| Paralela (4 workers)     | 445,9 MB    | 24,852    | 1,85x   | 46,2%      |
| Paralela (8 workers) \*  | 222,9 MB    | 23,984    | 1,92x   | 24,0%      |
| Paralela (10 workers) \* | 178,4 MB    | 23,953    | 1,92x   | 19,2%      |
| Paralela (20 workers) \* | 89,2 MB     | 22,614    | 2,03x   | 10,2%      |

`*` workers > núcleos físicos (4) -> oversubscription

**Observações gerais:**

- O ganho de todas as configurações paralelas ficou em torno de **1,85x–2,03x**, bem abaixo do speedup teórico máximo de 4x (igual ao número de núcleos).
- A eficiência cai drasticamente com o aumento do número de workers, de 94,5% com 2 workers para apenas 10,2% com 20 workers.
- A diferença de tempo entre 2 e 20 workers é de apenas ~1,7 s, evidenciando que o fator limitante é o acesso ao disco, não a CPU.

---

## 7. Discussão

### 7.1 A versão paralela foi mais rápida?

**Sim.** O melhor resultado foi com 20 workers: **22,614 s** contra **45,955 s** do sequencial.

- Speedup máximo atingido: **2,03x**
- Redução no tempo total: **50,8%**

Porém, o ganho ficou muito aquém do esperado para uma máquina com 4 núcleos (speedup teórico de 4x com 4 workers). O gargalo principal identificado foi a **contenção de I/O**: múltiplos processos lendo o mesmo arquivo HDD/SSD simultaneamente limitam o throughput de leitura.

### 7.2 O tamanho do chunk influenciou?

**Sim**, de duas formas interdependentes:

- **Chunks maiores** (menos workers) -> menor overhead de fork/join e menos contenção de I/O, mas sub-utilização dos núcleos.
- **Chunks menores** (mais workers) -> maior grau de paralelismo, porém mais processos competem pelo mesmo arquivo, aumentam o custo de serialização via pickle e, em oversubscription, geram context-switch frequente no kernel.

**Exemplo concreto do log:**

| Workers | Chunk médio | Tempo    |
| ------- | ----------- | -------- |
| 2       | 891,8 MB    | 24,318 s |
| 4       | 445,9 MB    | 24,852 s |
| 20      | 89,2 MB     | 22,614 s |

O fato de 20 workers ter sido o mais rápido mesmo com oversubscription sugere que **blocos menores melhoram a localidade de cache do kernel** (page cache), resultando em leituras mais eficientes do que com blocos grandes que extrapolam o cache disponível.

### 7.3 Houve custo extra de criação de processos?

**Sim.** Três fontes de overhead foram mensuráveis durante a execução:

| Fonte                                 | Impacto observado                                                                                                                                                                                 |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Fork do SO**                        | Cada `Pool(n)` executa `n` chamadas `fork()`. Com 20 processos, o acúmulo estimado é de 100–400 ms antes de qualquer leitura começar.                                                             |
| **Pickle (serialização)**             | Argumentos `(filepath, start, end)` e o dicionário `Stats` retornado são serializados/desserializados a cada chamada de `pool.map()`. Para dados simples como este caso, o impacto é desprezível. |
| **Context-switch (oversubscription)** | Com 8 e 10 workers, o overhead medido foi de **+1,370 s** e **+1,339 s** respectivamente em relação ao melhor resultado (20 workers).                                                             |

É notável que, diferente do esperado, o **overhead de oversubscription não foi crescente** com o número de workers — 20 workers foi mais rápido que 8 e 10. Isso reforça que o page cache do kernel teve papel decisivo nos resultados.

### 7.4 Em que cenário a paralelização compensa?

Com base nos resultados obtidos:

**Compensa:**

- Processamento **CPU-bound** (parse, cálculo, transformação intensiva) onde o custo de computação por linha seja alto o suficiente para amortizar o fork e a contenção de I/O.
- Arquivo em **storage de alta velocidade** (SSD NVMe, RAM disk) — eliminaria o principal gargalo observado neste experimento.
- Blocos **independentes**, sem estado compartilhado entre workers (como neste script).
- Número de workers **<= núcleos físicos** para maximizar eficiência (94,5% com 2 workers vs. 10,2% com 20).

**Não compensa:**

- Arquivo em **HDD ou rede lenta** (I/O-bound domina — exatamente o que foi observado).
- Dataset pequeno, onde o overhead de criação do pool supera o ganho.
- Workers excedendo os núcleos físicos quando o objetivo é maximizar eficiência (a eficiência caiu de 94,5% para 10,2%).

**Para escalar além de uma única máquina:**

| Ferramenta          | Característica                                                    |
| ------------------- | ----------------------------------------------------------------- |
| **Ray**             | Multiprocessing distribuído com API similar ao `Pool`             |
| **Dask**            | DataFrame com leitura de CSV em chunks nativos e avaliação lazy   |
| **Spark (PySpark)** | Clusters com shuffle distribuído para datasets de escala petabyte |

---

## 8. Conclusão

O experimento demonstrou com sucesso o paradigma **Divisão e Conquista** aplicado ao processamento de um arquivo CSV de 1,78 GB contendo 11.382.049 registros.

**Resultados-chave:**

- A versão paralela reduziu o tempo de processamento de **45,955 s para 22,614 s** (melhor caso: 20 workers), um speedup de **2,03x** e redução de **50,8%** no tempo total.
- A **corretude foi preservada** em todas as configurações: mesmo número de viagens processadas, zero erros de parsing e valores agregados praticamente idênticos (pequenas diferenças na 2ª casa decimal decorrem de acumulação de ponto flutuante em ordens distintas).
- O **gargalo principal** não foi a CPU, mas o **throughput de I/O do disco**: múltiplos processos lendo o mesmo arquivo físico simultaneamente geraram contenção que limitou o speedup a ~2x, muito abaixo do teórico de 4x.
- A **eficiência por worker** degradou de 94,5% (2 workers) para 10,2% (20 workers), evidenciando que adicionar processos além da capacidade física não escala linearmente neste tipo de workload.

O maior aprendizado prático é que, para workloads I/O-bound como este, **o hardware de armazenamento é o fator dominante** — migrar o arquivo para um SSD NVMe ou RAM disk provavelmente aproximaria o speedup do teórico de 4x.
