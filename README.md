# Trabalho 02 — Paralelismo Parte C

**Divisão e Conquista no Processamento Massivo de Dados**

Este projeto demonstra o paradigma **Divisão e Conquista** aplicado ao processamento de um arquivo CSV volumoso (dados de corridas de táxi de Nova York — fevereiro/2016), comparando uma abordagem sequencial com múltiplas configurações paralelas usando `multiprocessing` do Python.

---

## Sumário

- [Trabalho 02 — Paralelismo Parte C](#trabalho-02--paralelismo-parte-c)
  - [Sumário](#sumário)
  - [Pré-requisitos](#pré-requisitos)
  - [Instalação](#instalação)
  - [Dataset](#dataset)
  - [Como executar](#como-executar)
  - [Visão geral do código](#visão-geral-do-código)
  - [Etapa 1 — Dividir](#etapa-1--dividir)
  - [Etapa 2 — Conquistar](#etapa-2--conquistar)
  - [Etapa 3 — Combinar](#etapa-3--combinar)
  - [Etapa 4 — Comparar (Sequencial vs. Paralela)](#etapa-4--comparar-sequencial-vs-paralela)
    - [4a — Versão Sequencial (`run_sequential`)](#4a--versão-sequencial-run_sequential)
    - [4b — Versão Paralela (`run_parallel`)](#4b--versão-paralela-run_parallel)
  - [Etapa 5 — Discutir](#etapa-5--discutir)
    - [1. A versão paralela foi mais rápida?](#1-a-versão-paralela-foi-mais-rápida)
    - [2. O tamanho do chunk influenciou?](#2-o-tamanho-do-chunk-influenciou)
    - [3. Houve custo extra de criação de processos?](#3-houve-custo-extra-de-criação-de-processos)
    - [4. Em que cenário a paralelização compensa?](#4-em-que-cenário-a-paralelização-compensa)
  - [Métricas coletadas](#métricas-coletadas)
  - [Saída esperada](#saída-esperada)

---

## Pré-requisitos

| Ferramenta | Versão mínima |
| ---------- | ------------- |
| Python     | 3.11          |
| Poetry     | 2.0           |

> O projeto usa apenas a biblioteca padrão do Python para o processamento (`csv`, `multiprocessing`, `os`, `time`). O `ipython` é uma dependência opcional para exploração interativa.

---

## Instalação

```bash
# 1. Clone ou acesse o diretório do projeto
cd trabalho_02_paralelismo_parte_c

# 2. Instale as dependências (incluindo ferramentas de desenvolvimento)
poetry install --with dev

# 3. Ative o ambiente virtual (opcional)
poetry shell
```

---

## Dataset

Arquivo esperado na raiz do projeto:

```
yellow_tripdata_2016-02.csv
```

O arquivo contém dados de corridas do táxi amarelo de Nova York referentes a fevereiro de 2016. As colunas utilizadas pelo script são:

| Índice | Nome da coluna    | Descrição                     |
| ------ | ----------------- | ----------------------------- |
| 3      | `passenger_count` | Número de passageiros         |
| 4      | `trip_distance`   | Distância da corrida (milhas) |
| 12     | `fare_amount`     | Tarifa base (US$)             |
| 15     | `tip_amount`      | Gorjeta (US$)                 |
| 18     | `total_amount`    | Valor total cobrado (US$)     |

> O cabeçalho da primeira linha é ignorado automaticamente durante o processamento.

---

## Como executar

```bash
# Dentro do diretório do projeto (com o ambiente Poetry ativo):
python .

# Ou explicitamente:
python __main__.py
```

A execução roda automaticamente:

1. A versão **sequencial** (baseline).
2. A versão **paralela** para cada configuração de workers: `[2, 4, 8, 10, 20]`.
3. A **tabela comparativa** de desempenho.
4. A **discussão** analítica dos resultados.

---

## Visão geral do código

```
__main__.py
│
├── get_chunks()          # Etapa 1 — Dividir
├── process_chunk()       # Etapa 2 — Conquistar
├── aggregate()           # Etapa 3 — Combinar
├── run_sequential()      # Etapa 4a — Baseline sequencial
├── run_parallel()        # Etapa 4b — Execução paralela (Pool)
├── print_comparison_table()  # Etapa 5 — Tabela comparativa
└── print_discussion()    # Etapa 5 — Discussão
```

---

## Etapa 1 — Dividir

**Função:** `get_chunks(filepath, n_chunks) → List[Tuple[int, int]]`

O arquivo CSV é particionado em `n_chunks` intervalos definidos por pares `(start, end)` de **byte-offsets**, sem carregar o arquivo inteiro na memória.

**Estratégia de alinhamento:**

1. Lê o cabeçalho para descobrir seu tamanho em bytes e excluí-lo dos blocos.
2. Calcula o tamanho aproximado de cada bloco: `chunk_size = data_size // n_chunks`.
3. Para cada bloco (exceto o último), avança o ponteiro até o final da linha mais próxima — garantindo que nenhuma linha de dado seja cortada ao meio.
4. O último bloco sempre vai até o final do arquivo.

Isso permite que cada processo leia um intervalo distinto do mesmo arquivo **sem sobreposição e sem sincronização**, viabilizando o paralelismo real.

---

## Etapa 2 — Conquistar

**Função:** `process_chunk(args: Tuple[str, int, int]) → Stats`

Cada worker recebe `(filepath, start, end)` e processa exclusivamente seu bloco:

1. Abre o arquivo em modo binário e posiciona o ponteiro em `start` via `f.seek(start)`.
2. Usa um **gerador de linhas** (`line_gen`) que decodifica e entrega somente as linhas dentro do intervalo `[start, end)` — evitando carregar o bloco inteiro em memória.
3. Alimenta um `csv.reader` com esse gerador para parsear os campos corretamente.
4. Acumula as métricas em um dicionário `Stats` local ao processo.

Esta função é executada em um **processo separado** no modo paralelo, contornando o GIL (Global Interpreter Lock) do CPython e aproveitando múltiplos núcleos de CPU.

---

## Etapa 3 — Combinar

**Função:** `aggregate(partial_list: List[Stats]) → Stats`

Após todos os workers concluírem, esta função aplica o **reduce**: itera sobre a lista de `Stats` parciais (um por bloco) e os soma em um único `Stats` global, seguindo o padrão **map-reduce**:

- `map` → `process_chunk()` aplicado em paralelo a cada bloco.
- `reduce` → `aggregate()` somando todos os resultados.

---

## Etapa 4 — Comparar (Sequencial vs. Paralela)

### 4a — Versão Sequencial (`run_sequential`)

Processa o arquivo inteiro em um único processo, linha a linha, usando `csv.reader` padrão. Serve como **baseline** para medir o ganho de desempenho da versão paralela.

### 4b — Versão Paralela (`run_parallel`)

Aplica as três etapas anteriores de forma integrada:

```
DIVIDIR    → get_chunks()     — particiona em n_workers blocos
CONQUISTAR → process_chunk()  — cada worker processa seu bloco (map)
COMBINAR   → aggregate()      — une os resultados (reduce)
```

Usa `multiprocessing.Pool` para criar **processos reais** (não threads), um por worker. As configurações de workers testadas são:

| Workers | Observação                            |
| ------- | ------------------------------------- |
| 2       | Muito abaixo da capacidade física     |
| 4       | Dentro da capacidade típica           |
| 8       | Próximo ou igual ao número de núcleos |
| 10      | Pode gerar oversubscription           |
| 20      | Oversubscription garantida            |

> Configurações com `workers > núcleos físicos` são marcadas com `*` na tabela e caracterizam **oversubscription**.

---

## Etapa 5 — Discutir

A discussão, impressa ao final da execução, responde quatro perguntas com base nos dados reais coletados:

### 1. A versão paralela foi mais rápida?

Identifica o melhor cenário paralelo e calcula o **speedup** (`tempo_sequencial / tempo_paralelo`) e a **redução percentual** no tempo total. Se nenhuma configuração superar o sequencial, indica que o overhead de I/O e fork dominou o ganho computacional.

### 2. O tamanho do chunk influenciou?

Explica a relação entre o número de workers e o tamanho dos blocos:

- **Chunks maiores** (menos workers) → menos overhead de fork/join, mas sub-utilização dos núcleos.
- **Chunks menores** (mais workers) → maior paralelismo, porém mais contenção de I/O e custo de serialização.

### 3. Houve custo extra de criação de processos?

Descreve as três fontes de overhead mensuráveis:

| Fonte                     | Descrição                                                                                                |
| ------------------------- | -------------------------------------------------------------------------------------------------------- |
| **Fork do SO**            | Cada `Pool(n)` executa `n` chamadas `fork()`, copiando o espaço de memória do processo pai.              |
| **Pickle (serialização)** | Argumentos e resultados são serializados/desserializados em cada chamada a `pool.map()`.                 |
| **Context-switch**        | Com `workers > núcleos`, o kernel alterna processos no mesmo núcleo, adicionando latência de cache miss. |

### 4. Em que cenário a paralelização compensa?

| Compensa ✔                                      | Não compensa ✘                                  |
| ----------------------------------------------- | ----------------------------------------------- |
| Processamento CPU-bound                         | Arquivo em HDD ou rede lenta (I/O-bound domina) |
| Storage de alta velocidade (SSD NVMe, RAM disk) | Dataset pequeno (overhead > ganho)              |
| Blocos independentes sem estado compartilhado   | Workers > núcleos físicos (oversubscription)    |
| `workers ≤ núcleos físicos`                     | —                                               |

Alternativas para escalar além de uma única máquina mencionadas:

- **Ray** — multiprocessing distribuído com API similar ao `Pool`
- **Dask** — DataFrame com leitura de CSV em chunks e avaliação lazy
- **Spark (PySpark)** — clusters com shuffle distribuído

---

## Métricas coletadas

Para cada bloco (e no total após o combine), são calculadas:

| Métrica            | Descrição                              |
| ------------------ | -------------------------------------- |
| `trip_count`       | Número total de viagens processadas    |
| `errors`           | Linhas que geraram erro de parsing     |
| `total_distance`   | Soma das distâncias (milhas)           |
| `total_fare`       | Soma das tarifas base (US$)            |
| `total_tip`        | Soma das gorjetas (US$)                |
| `total_amount`     | Soma dos valores totais cobrados (US$) |
| `total_passengers` | Soma do número de passageiros          |

Ao final, são exibidas as **médias por viagem** para distância, tarifa, gorjeta e passageiros, além da receita total acumulada.

---

## Saída esperada

```
======================================================================
  Divisão e Conquista — Processamento Massivo de Dados
======================================================================
  Arquivo         : yellow_tripdata_2016-02.csv
  Tamanho         : X.XX GB
  Núcleos físicos : N
  Workers testados: [2, 4, 8, 10, 20]

[0] Executando versão SEQUENCIAL (baseline)…

==============================================================
  RESULTADO — SEQUENCIAL
==============================================================
  Viagens processadas  :        XX,XXX,XXX
  Erros de parsing     :                 0
  Distância média (mi) :            X.XXXX
  Tarifa média  (US$)  :           XX.XXXX
  Gorjeta média (US$)  :            X.XXXX
  Receita total (US$)  :    XXX,XXX,XXX.XX
  Pass. médio/viagem   :            X.XXXX
  Tempo de execução    :           XX.XXX s

[1/5] Executando versão PARALELA — 2 workers…
...

======================================================================
  TABELA COMPARATIVA — SEQUENCIAL vs. PARALELA
======================================================================
  Configuração           Chunk médio    Tempo (s)   Speedup  Eficiência
  ---------------------- ------------ ---------- --------- -----------
  Sequencial (1 proc)               —     XX.XXX     1.00x      100.0%
  Paralela ( 2 workers)     XXX.X MB     XX.XXX     X.XXx       XX.X%
  ...
  Paralela (20 workers) *   XXX.X MB     XX.XXX     X.XXx       XX.X%

  * workers > núcleos físicos (N) → oversubscription
======================================================================
```
