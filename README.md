# eEDB-022-03-R

Pipeline de **Ingestão e ETL em camadas (RAW → Trusted → Delivery)** usando **R + Spark** (via [`sparklyr`](https://spark.rstudio.com/)), com persistência em **Parquet** e no **PostgreSQL**.

Este projeto é a versão em **R** da Atividade 3 (existe também uma versão em Python + PySpark do mesmo pipeline). Todo o tratamento de dados é feito pela **DataFrame API do Spark** — nenhuma etapa usa SQL cru.

**Integrantes**
- Antonio Daniel de Souza Linhares
- Yuri Alexandre Barbosa Rodrigues
- Hercules Ramos Veloso de Freitas

---

## Sumário

- [Visão geral do pipeline](#visão-geral-do-pipeline)
- [Estrutura do repositório](#estrutura-do-repositório)
- [Pré-requisitos](#pré-requisitos)
- [Configuração](#configuração)
- [Como executar](#como-executar)
- [Detalhe das camadas](#detalhe-das-camadas)
- [Limitações conhecidas](#limitações-conhecidas)
- [Sugestões de melhoria](#sugestões-de-melhoria)

---

## Visão geral do pipeline

```
Fontes originais (CSV/TSV)
        │
        ▼
┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│      RAW      │ ──▶ │    Trusted    │ ──▶ │   Delivery    │
│  espelho fiel │     │ tipagem, dedup│     │ tabela final  │
│  da origem    │     │ e chaves      │     │ banco×trimestre│
└───────────────┘     └───────────────┘     └───────────────┘
   Parquet + schema      Parquet + schema      Parquet + tabela
   "raw" no Postgres     "trusted" no Postgres  "delivery" no Postgres
```

| Etapa | Script | O que faz |
|---|---|---|
| 1. RAW | `01_ingest_raw.R` | Lê Reclamações, Bancos e Empregados exatamente como vieram (mesmos nomes de coluna, tudo como `string`), grava em `data/raw/*.parquet` e no schema `raw` do Postgres. |
| 2. Trusted | `02_trusted.R` | Tipagem, normalização de CNPJ, deduplicação e resolução de nomes via `dplyr`/Spark DataFrame API. Grava em `data/trusted/*.parquet` e no schema `trusted`. |
| 3. Delivery | `03_delivery.R` | Junta as três tabelas Trusted por CNPJ e materializa `delivery.tb_reclamacoes_bancos_funcionarios`. |

## Estrutura do repositório

```
.
├── 01_ingest_raw.R     # Etapa 1 — ingestão RAW (Reclamações, Bancos, Empregados)
├── 02_trusted.R        # Etapa 2 — tratamento e normalização (camada Trusted)
├── 03_delivery.R        # Etapa 3 — join final e tabela de entrega
├── db.R                # Conexão JDBC com o PostgreSQL (leitura/escrita)
├── fuzzy_match.R        # Matching aproximado de nomes de instituições
├── spark_session.R     # Configuração da SparkSession (via sparklyr)
├── run_all.R            # Orquestra as 3 etapas em sequência
└── .gitignore
```

## Pré-requisitos

- **R** ≥ 4.1
- **Java** 8 ou 11 (exigido pelo Spark)
- **Apache Spark** (instalado automaticamente pelo `sparklyr::spark_install()`, se necessário)
- **PostgreSQL** acessível (local, Docker ou remoto)
- Pacotes R:
  ```r
  install.packages(c("sparklyr", "dplyr"))
  sparklyr::spark_install(version = "3.4")  # ajuste a versão conforme o seu ambiente
  ```

## Configuração

O pipeline é 100% configurável por variáveis de ambiente, com valores padrão sensatos para rodar localmente ou em Docker:

| Variável | Padrão | Descrição |
|---|---|---|
| `SPARK_MASTER` | `local[4]` | Master do Spark. `local[4]` usa 4 núcleos; use `local[*]` para todos os núcleos disponíveis, ou `local[2]` em máquinas mais fracas. |
| `RAW_ORIGEM_DIR` | `/opt/data/raw/origem` | Pasta com os arquivos-fonte (Reclamações, Bancos, Empregados). |
| `RAW_OUT_DIR` | `/opt/data/raw` | Saída em Parquet da camada RAW. |
| `TRUSTED_OUT_DIR` | `/opt/data/trusted` | Saída em Parquet da camada Trusted. |
| `DELIVERY_OUT_DIR` | `/opt/data/delivery` | Saída em Parquet da camada Delivery. |
| `DB_HOST` | `postgres` | Host do PostgreSQL. |
| `DB_PORT` | `5432` | Porta do PostgreSQL. |
| `DB_USER` | `postgres` | Usuário do PostgreSQL. |
| `DB_PASSWORD` | `postgres` | Senha do PostgreSQL. |
| `DB_NAME` | `eedb` | Nome do banco de dados. |

Exemplo de `.Renviron` (não versionado) para rodar localmente:

```
SPARK_MASTER=local[2]
RAW_ORIGEM_DIR=./data/raw/origem
RAW_OUT_DIR=./data/raw
TRUSTED_OUT_DIR=./data/trusted
DELIVERY_OUT_DIR=./data/delivery
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=eedb
```

## Como executar

### Pipeline completo

```r
source("run_all.R")
```

Isso executa, em sequência: `01_ingest_raw.R` → `02_trusted.R` → `03_delivery.R`, imprimindo o progresso de cada etapa no console.

### Executando uma etapa isoladamente

```r
source("01_ingest_raw.R"); run_ingest_raw()
source("02_trusted.R");    run_trusted()
source("03_delivery.R");   run_delivery()
```

> ⚠️ Ver [Limitações conhecidas](#limitações-conhecidas) — no estado atual do repositório, `run_delivery()` depende de tabelas Trusted que `02_trusted.R` ainda não grava.

### Via linha de comando

```bash
Rscript run_all.R
```

## Detalhe das camadas

### `01_ingest_raw.R` — RAW
- **Reclamações**: `*.csv`, separador `;`, encoding `ISO-8859-1`; adiciona `arquivo_origem` via `input_file_name()`.
- **Bancos**: `EnquadramentoInicia_v2.tsv`, separador `\t`, encoding `ISO-8859-1`.
- **Empregados**: dois arquivos (`match` e `match_less`) do Glassdoor, separador `|`, encoding `UTF-8`, concatenados sem deduplicação (a dedup acontece na Trusted).
- Todas as colunas são lidas como `string` (`infer_schema = FALSE`) — nenhuma regra de negócio é aplicada nesta etapa.
- Grava em Parquet (`spark_write_parquet`) **e** no Postgres (`write_table_jdbc`, schema `raw`).

### `02_trusted.R` — Trusted
- `tratar_bancos()`: normaliza o CNPJ para raiz de 8 dígitos (`regexp_replace` + `lpad`), limpa nome e segmento, e deduplica por CNPJ priorizando o registro com "PRUDENCIAL" no nome (via `window_order` + `row_number`). O nome do segundo colocado é preservado em `nome_alternativo`.
- `fuzzy_match.R` é carregado (`source`) para dar suporte à resolução de nomes de bancos "conglomerado" por matching aproximado.

### `03_delivery.R` — Delivery
- Lê `trusted.bancos`, `trusted.reclamacoes` e `trusted.empregados`.
- Filtra reclamações com CNPJ válido, faz `inner_join` com bancos e `left_join` com empregados.
- Cria a flag `possui_avaliacao_glassdoor`.
- Grava `delivery.tb_reclamacoes_bancos_funcionarios` em Parquet e no Postgres.

## Limitações conhecidas

- **`02_trusted.R` só implementa `tratar_bancos()`.** Não há, no momento, funções equivalentes para tratar `reclamacoes` e `empregados` (tipagem, deduplicação e resolução de CNPJ por nome), embora `03_delivery.R` já espere ler `trusted.reclamacoes` e `trusted.empregados`. Isso faz `run_delivery()` falhar após um `run_all.R` "limpo", pois essas tabelas nunca são criadas.
- **`fuzzy_match.R` é carregado mas não é chamado** em nenhum ponto visível de `02_trusted.R` — a lógica de matching aproximado de nomes de bancos "conglomerado" ainda não está conectada ao fluxo de `tratar_reclamacoes`.
- Não há testes automatizados nem validação de schema/qualidade de dados entre camadas.
- Não há `Dockerfile`/`docker-compose.yml` neste repositório (diferente da versão em Python), então subir Postgres + Spark localmente depende de configuração manual.

## Sugestões de melhoria

1. **Completar a camada Trusted**: implementar `tratar_reclamacoes()` e `tratar_empregados()` em `02_trusted.R` (tipagem de colunas, `unionByName` dos dois arquivos de Empregados com deduplicação por CNPJ, e resolução de CNPJ de bancos "conglomerado" — exato por nome normalizado, com fallback fuzzy usando `fuzzy_match.R`), gravando as três tabelas Trusted antes de rodar `03_delivery.R`.
2. **Conectar `fuzzy_match.R` ao pipeline**, com um parâmetro (variável de ambiente, ex. `MATCH_STRATEGY`) para escolher a estratégia de resolução de nomes, deixando a porta aberta para uma futura estratégia probabilística.
3. **Adicionar `docker-compose.yml`** com serviços `postgres` e o próprio job R, replicando a facilidade de execução da versão Python.
4. **Adicionar testes** (`testthat`) para as funções de tratamento (normalização de CNPJ, deduplicação, joins), e um `renv.lock` para travar as versões dos pacotes R.
5. **Registrar logs estruturados** por etapa (linhas lidas/gravadas, tempo de execução) em vez de apenas `cat()`.
