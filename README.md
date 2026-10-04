# Case Analytics Engineering · Anti-Fraud & Risk

Proposta de arquitetura, modelagem, qualidade e governança de dados para um time de Risco acompanhar o
motor de fraude, a perda por chargeback por safra e o impacto das regras na taxa de aprovação.

| Caminho | Conteúdo |
|---|---|
| [`fraud-risk-analytics/`](fraud-risk-analytics/) | Protótipo executável: dbt + DuckDB, PySpark com teste de paridade e variáveis de velocidade, data contracts, ingestão no Lakeflow e jobs (Databricks Asset Bundle) |
| [`fraud-risk-analytics/docs/decisoes.md`](fraud-risk-analytics/docs/decisoes.md) | Decisões de arquitetura e modelagem, com alternativas consideradas |
| [`.github/workflows/ci.yml`](.github/workflows/ci.yml) | CI: lint SQL, dbt build (modelos, testes de dado, unit tests) e paridade PySpark |

## Rodar tudo

Requisitos: Python 3.10+ e, para a parte PySpark, Java 8+ no PATH.

```powershell
# Windows (PowerShell)
.\run_all.ps1            # -SkipSpark para pular PySpark
```

```bash
# Linux / macOS
./run_all.sh             # SKIP_SPARK=1 ./run_all.sh para pular PySpark
```

O script cria `.venv`, instala dependências, carrega os dados do case, roda `dbt build`, a simulação de
chargeback tardio, o lint SQL e os testes PySpark. Resultado esperado:

- `dbt build`: `PASS=109 WARN=2 ERROR=0`. Os 2 avisos são achados do dado do case: `tx_1005` aprovada sem
  avaliação de risco e duplicata de `tx_1004`.
- Simulação: chargeback de D+120 leva o CB rate da safra 01/03 (cartão) de 17,65% para 58,82%,
  reprocessando só essa safra.
- PySpark: `6 passed`. Fato e agregado idênticos aos do dbt, incremental com sobreposição e variáveis de
  velocidade por cliente.

Detalhes do protótipo em [`fraud-risk-analytics/README.md`](fraud-risk-analytics/README.md) e decisões em
[`fraud-risk-analytics/docs/decisoes.md`](fraud-risk-analytics/docs/decisoes.md).
