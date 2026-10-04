# Case Analytics Engineering · Anti-Fraud & Risk

Proposta de arquitetura, modelagem, qualidade e governança de dados para um time de Risco acompanhar o
motor de fraude, a perda por chargeback por safra e o impacto das regras na taxa de aprovação.

| Arquivo | O que é |
|---|---|
| [`case-analytics-engineering-fraude.html`](case-analytics-engineering-fraude.html) | Apresentação. Abra no navegador e use o botão **Apresentar** (ou tecla `P`) para o modo slide |
| [`guia-conceitos-fraude-e-risco.html`](guia-conceitos-fraude-e-risco.html) | Guia de conceitos: pagamento com cartão, chargeback, reason codes, Pix/MED, motor de risco, safras e termos de engenharia de dados |
| [`fraud-risk-analytics/`](fraud-risk-analytics/) | Protótipo executável: dbt + DuckDB, implementação PySpark com teste de paridade, data contracts e docs |
| [`case-arquitetura.html`](case-arquitetura.html) | Arquitetura passo a passo: ingestão, camadas, dbt e PySpark, orquestração e falhas |
| [`.github/workflows/ci.yml`](.github/workflows/ci.yml) | CI: lint SQL → dbt build (modelos, testes de dado, unit tests) → paridade PySpark |

## Rodar tudo

Requisitos: Python 3.10+ e, para a parte PySpark, Java 8+ no PATH.

```powershell
# Windows (PowerShell)
.\run_all.ps1            # -SkipSpark para pular PySpark, -NoOpen para não abrir o navegador
```

```bash
# Linux / macOS
./run_all.sh             # SKIP_SPARK=1 ./run_all.sh para pular PySpark
```

O script cria `.venv`, instala dependências, carrega os dados do case, roda `dbt build`, a simulação de
chargeback tardio, o lint SQL e os testes PySpark. Resultado esperado:

- `dbt build`: `PASS=109 WARN=2 ERROR=0`. Os 2 avisos são achados reais do dado: `tx_1005` aprovada sem
  avaliação de risco e duplicata de `tx_1004`.
- Simulação: chargeback de D+120 leva o CB rate da safra 01/03 (cartão) de 17,65% para 58,82%,
  reprocessando só essa safra.
- PySpark: `5 passed`, fato e agregado idênticos aos do dbt e incremental com sobreposição testado.

Detalhes do protótipo em [`fraud-risk-analytics/README.md`](fraud-risk-analytics/README.md) e decisões em
[`fraud-risk-analytics/docs/decisoes.md`](fraud-risk-analytics/docs/decisoes.md).
