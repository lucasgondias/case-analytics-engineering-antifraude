# fraud-risk-analytics

Protótipo executável do case de Analytics Engineering, Anti-Fraud & Risk.
Projeto dbt sobre DuckDB, com os dados do enunciado como Bronze, o fluxo staging → intermediate → marts → camada semântica, testes que pegam as armadilhas do dado e uma simulação de chargeback tardio.

Portabilidade: funções que diferem entre bancos ficam em macros (`dbt.datediff`, `dbt.dateadd`, `dbt.listagg` e `macros/cross_db.sql`); o restante usa sintaxe comum a DuckDB e Databricks SQL. Validado em DuckDB; o alvo de produção é dbt-databricks.

## Como rodar

```bash
python -m venv .venv && .venv/Scripts/activate      # Linux/Mac: source .venv/bin/activate
pip install -r requirements.txt
export DBT_PROFILES_DIR=.

dbt seed                                   # Bronze: dados do case
dbt build --exclude resource_type:seed     # modelos + testes de dado + unit tests
python scripts/simulate_late_chargeback.py # chargeback D+120 reabre só a safra afetada
sqlfluff lint models tests/singular
python -m pytest spark/tests               # PySpark: paridade com o dbt (requer Java 8+)
python spark/run_pipeline.py               # PySpark: carga + chargeback D+120 incremental
```

Resultado esperado: `PASS=109 WARN=2 ERROR=0` (35 modelos, 67 testes de dado, 8 unit tests, 1 snapshot). Os 2 WARN são achados do case, não falhas do pipeline:

| Teste | Linha pega | Significado |
|---|---|---|
| `assert_approved_transactions_have_risk_evaluation` | tx_1005 | Aprovada sem avaliação de risco (motor aprovou sem avaliar, por exemplo após timeout, ou o feed de avaliações perdeu o evento) |
| `assert_no_duplicate_payment_candidates` | tx_1005 | Mesmo usuário, valor e método no mesmo segundo que tx_1004, com id diferente |

## Estrutura

```
seeds/                  Bronze do enunciado (raw_*, idêntico ao case) + referências (reason codes, códigos do emissor)
seeds/v2/               fontes propostas no contrato v2, com dados ILUSTRATIVOS: enriquecimento do pagamento
                        (lojista, pedido, país, bandeira, 3DS, código do emissor, liquidação, prazo de repasse),
                        lojistas, TC40/SAFE, reembolsos, alertas pré-disputa, MED do Pix, desfecho de disputa,
                        regras avaliadas (com shadow mode)
models/staging/         tipagem, dedup técnico, normalização (1:1 com a fonte)
models/intermediate/    decisão de risco vigente, chargeback por tx, suspeitas de duplicidade
models/marts/           fct_payment_attempts (contract enforced), fct_chargebacks, fct_fraud_labels (label store),
                        agregados por safra, ratio das bandeiras por lojista (VAMP/ECM/EFM), perda por lojista,
                        exposição, backtest de regras
models/monitoring/      saúde diária (z-score, fail-open), PSI do score, z-score por regra, curva de maturação
snapshots/              histórico "como reportado" do agregado por safra
models/semantic/        métricas oficiais (MetricFlow): definidas uma vez, consumidas por todos
models/_unit_tests.yml  lógica testada com fixture: safra, origem da recusa, reason code, duplicidade
tests/singular/         erros silenciosos + reconciliação Bronze ↔ Gold
contracts/              data contracts ODCS v3 com os produtores (adquirente, motor de risco)
../.github/workflows/   CI na raiz do repositório: lint → build (Slim CI) → paridade PySpark
docs/decisoes.md        decisões, premissas e perguntas em aberto para Risco
spark/                  mesma lógica em PySpark: transformações puras + I/O Delta + teste de paridade
```

## dbt e PySpark

dbt organiza a transformação (testes, contratos, CI, linhagem, camada semântica) e Spark executa.
A regra de negócio está nas duas implementações e `spark/tests/test_parity.py` compara linha a linha
`fct_payment_attempts` e `agg_chargeback_cohort_daily` do PySpark com a saída do dbt.

- `spark/fraud_risk_spark/transformations.py`: funções puras de DataFrame, testáveis em memória. Cobre o núcleo
  (staging, decisão de risco, duplicidade, fato de tentativas, agregado por safra e incremental com sobreposição
  e vizinhos). Os modelos do contrato v2 existem só no dbt.
- `spark/fraud_risk_spark/io_delta.py`: escrita em produção (Delta `MERGE` e `replaceWhere`). Não roda
  no protótipo local, porque Delta no Windows exige Hadoop nativo; a lógica que ela grava é a testada.

## Correções da revisão especialista

| Bug | Correção | Prova |
|---|---|---|
| B1 watermark sem sobreposição perdia evento fora de ordem | `incremental_since()`: watermark − 6h, MERGE idempotente | unit test `incremental_picks_out_of_order_events_and_duplicate_neighbors` (dbt e PySpark) |
| B2 idade/maturidade congeladas no incremental | calculadas na leitura (`rpt_chargeback_cohort_maturity`) | coluna removida do agregado |
| B3 recusa por revisão caía como recusa do emissor | `decline_source = manual_review`; emissor soft/hard | unit test `decline_source_and_silent_failure_flags` |
| B4 duplicata nova não reabria a antiga | vizinhos do mesmo usuário entram nas chaves afetadas | mesmo unit test de incremental |

## Números do case (saída de `rpt_metric_definition_sensitivity`)

| Definição | Aprovação | CB qtd | CB R$ |
|---|---|---|---|
| Contrato literal | 80,00% | 25,00% | 7,32% |
| Sem duplicidade | 75,00% | 33,33% | 8,82% |
| Só cartão | 75,00% | 33,33% | 17,65% |
| Cartão e sem duplicidade | 66,67% | 50,00% | 30,00% |

O dado é o mesmo nas quatro linhas e os números são diferentes. Por isso a métrica vive na camada semântica, versionada.
