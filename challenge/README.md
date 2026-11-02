# WeCogno Data Engineering Challenge

**Autor:** Daniel Santos  
**Projeto:** ETL de táxis de Nova York — Engenharia de Dados  
**Data de criação:** 01/11/2026

# ETL de táxis de Nova York

Para atender ao desafio fiz a subida do:
- Airflow 2.11 via docker,
- Postgress 16 via docker também,

Considerando a ingestão dos 12 arquivos para o postgres para aplicar os tratamentos de dados, arquitetura medalhão (aqui usei a nomenclatura bronze=raw, silver=trusted e gold=refined).

## Executar

Para execute é necessário Docker com Compose v2, e ao menos 4 GB de RAM disponíveis (8 GB recomendados), espaço livre para imagens, tabelas e WAL (logs do Postgres) (reserve 60 GB).
No Windows, execute no terminal WSL integrado ao Docker Desktop.

```bash
cd challenge
mkdir -p results
# Linux/WSL: permitir ao usuário do container escrever o relatório
printf 'AIRFLOW_UID=%s\n' "$(id -u)" > .env
docker compose up -d --build
docker compose ps -a
```
Para acessar o Airflow use a URL http://localhost:8080, entre com `admin` / `admin` e dispare a DAG
`nyc_taxi_2022`. A agenda é manual para evitar recarregar todo o ano sem intenção.
Também é possível executar a DAG pelo terminal:

```bash
docker compose exec airflow-scheduler airflow dags test nyc_taxi_2022 2022-01-01
```

O sucesso da última tarefa produz `results/answers.json` com as quatro respostas
e contagens de qualidade. Para consultar diretamente o banco:

```bash
docker compose exec -T postgres psql -U airflow -d taxi < sql/answers.sql
```

O banco fica disponível em localhost:5433 (banco `taxi`, usuário/senha `airflow`).
Credenciais simples e portas restritas a localhost destinam-se ao exercício local.
Para parar mantendo os dados: `docker compose down`.

## Processo e decisões

1. `extract_load_raw`: valida os 12 nomes esperados, descompacta um arquivo por
   vez em área temporária quando necessário e lê Parquet em lotes de 25 mil linhas. Embora os arquivos sejam `.gz`, eles são lidos como parquet, preservnado todas
   as colunas de origem como texto em `raw.trips`, com arquivo e número da linha.
2. `transform`: converte datas e medidas em `trusted.trips`; cria `refined.trips`
   com duração, campos analíticos e sinalizador de validade. A camada raw guarda
   os demais atributos para auditoria. Datas são timestamps locais sem conversão
   de fuso, conforme fornecidas pela fonte.
3. `quality_and_answers`: verifica a igualdade de contagens entre arquivos e
   camadas, contabiliza registros sinalizados e executa `sql/answers.sql`.

Cada etapa de banco usa transação: falhas desfazem a etapa. Reexecuções substituem
as tabelas e não acumulam linhas; uma única execução ativa da DAG evita concorrência.
Arquivos iguais entre si ou linhas idênticas não são deduplicados: a fonte não
fornece identificador único da corrida. A carga completa é adequada ao exercício;
em produção, usar partições, controle por checksum e cargas incrementais.

As respostas consideram todos os registros, preservando a população original.
Distâncias negativas, duração negativa e datas ausentes são sinalizadas, sem
remoção automática. Outliers positivos permanecem inclusive na média e no máximo.
`is_valid` é uma verificação estrutural, não uma garantia de plausibilidade física.

## Perguntas e SQL

As quatro consultas completas estão em [`sql/answers.sql`](sql/answers.sql).

1. Total: `count(*)` na tabela final.
2. Viagens iniciadas **e** finalizadas em **17 de junho de 2022**: ambos os
   timestamps no intervalo `[2022-06-17, 2022-06-18)`.
3. “Mais longa percorrida” significa **maior distância em milhas**, com data de
   início e todos os empates; não significa maior duração.
4. Média, desvio padrão **populacional** (`stddev_pop`), mínimo, máximo e quartis
   contínuos (`percentile_cont` para 25%, 50% e 75%). Nulos seguem as regras SQL
   dos agregados e não são convertidos para zero.

## Respostas verificadas

Execução completa da DAG concluída com sucesso em **01/11/2026** (horário de
São Paulo), usando os 12 arquivos do repositório. O relatório estruturado está em
[`results/answers.json`](results/answers.json).

| Pergunta / medida | Resultado |
| --- | ---: |
| Total na tabela final | **39.656.098** |
| Início e fim em 17/06/2022 | **123.887** |
| Dia de início da viagem com maior distância | **28/10/2022** |
| Maior distância | **389.678,46 milhas** |
| Média | **5,9593989681 milhas** |
| Desvio padrão populacional | **599,1907068230 milhas** |
| Mínimo | **0,00 milhas** |
| 1º quartil (25%) | **1,10 milha** |
| Mediana (50%) | **1,90 milha** |
| 3º quartil (75%) | **3,56 milhas** |

A corrida de maior distância tem início às **05:19** e fim às **05:32** de
28/10/2022.

A verificação confirmou **39.656.098** linhas na origem, raw, trusted e refined.
Foram sinalizados **13.613** registros pelas verificações estruturais descritas
acima; não foram removidos. O outlier máximo não é capturado apenas por essas
regras, pois tem distância positiva e fim posterior ao início.

Validações realizadas: construção da imagem, `docker compose config`, serviços
ativos e endpoint de saúde do Airflow, teste de integração aprovado e execução
completa das três tarefas da DAG em estado `success`.

## Testes

O teste de integração usa um banco separado, 12 arquivos sintéticos e duas
execuções completas para verificar idempotência, rastreabilidade, nulos,
limites de datas, sinalização de anomalias e as quatro respostas do relatório:

### Passo a passo
```bash
docker compose exec postgres createdb -U airflow taxi_test
docker compose run --rm --no-deps \
  -e TAXI_DATABASE_URL=postgresql://airflow:airflow@postgres/taxi_test \
  airflow-scheduler python -m unittest discover -s tests -v
```

Se o banco `taxi_test` já existir, execute apenas o segundo comando.

