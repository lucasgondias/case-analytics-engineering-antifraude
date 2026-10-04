# Decisões, premissas e perguntas em aberto

## Decisões de modelagem

| # | Decisão | Alternativa descartada | Por quê |
|---|---|---|---|
| D1 | Flags de qualidade **sinalizam** e nunca filtram linhas | Remover duplicatas e transações sem avaliação no staging | Remover sem registro esconde a falha que o enunciado descreve. A métrica oficial segue o contrato e as flags permitem auditar |
| D2 | Incremental por **chaves afetadas**, com watermark por fonte **e sobreposição de 6h** | Reprocessar sempre os últimos 90 dias / watermark puro | Janela fixa perde chargeback de D+91 a D+120. Watermark puro (`> max`) perde evento ingerido fora de ordem. A sobreposição é idempotente porque o MERGE é por chave |
| D3 | Duplicata nova **reabre a transação vizinha** do mesmo usuário | Reabrir só a transação com evento novo | Sem isso a flag de duplicidade da transação antiga fica desatualizada |
| D4 | Agregados guardam só **numeradores e denominadores** e só o que é **função do dado** | Gravar taxas e idade de safra | Média de taxas está errada quando o volume varia. Idade e maturidade dependem de "hoje" e congelariam nas safras não reprocessadas: calculadas na leitura (`rpt_chargeback_cohort_maturity`) |
| D5 | Decisão de risco = **última avaliação até a autorização** (+5s de tolerância) | Última avaliação de todas | Uma nova avaliação depois da autorização não decidiu nada |
| D6 | `decline_source`: motor, revisão (interna ou parceiro), emissor (soft/hard), erro técnico, desconhecido | Usar só `status` | É o único jeito de medir o impacto das regras na conversão. Recusa humana não é do motor; recusa soft do emissor é retentável |
| D7 | Chargeback classificado por **reason code da bandeira** | Todo chargeback = fraude | Desacordo comercial (Visa 13.x) não mede o motor |
| D8 | **Duas taxas de chargeback**: por safra (risco) e das bandeiras (VAMP/ECM, por lojista e mês de notificação) | Só a taxa por safra | São perguntas diferentes: "o motor errou em março?" x "vamos ser multados este mês?". A segunda tem denominador de liquidadas e grão de lojista |
| D9 | **Label store** (`fct_fraud_labels`): uma linha por evidência, rótulo = primeira evidência | Rótulo = chargeback | TC40/SAFE chega dias antes do chargeback e conta no VAMP. Também entram reembolso por fraude, alerta pré-disputa e MED. `label_at` evita vazamento no treino de modelo |
| D10 | Perda em três níveis: bruta, líquida (após disputa) e **não recuperada pela plataforma** | Só perda bruta | Numa plataforma, o chargeback é debitado do lojista; a plataforma perde quando o saldo do lojista não cobre |
| D11 | **Exposição** por lojista: valor já repassado (D+2/D+14/D+30) e ainda contestável | Ignorar prazo de repasse | O repasse antecipado ao lojista acontece antes de a janela de chargeback (até 120 dias) fechar |
| D12 | Regras com **versão e shadow mode** (`agg_rule_backtest`) | Avaliar regra só depois de ligada | Backtest de precisão, cobertura, custo em conversão e sobreposição antes de a regra decidir |
| D13 | **Snapshot "como reportado"** do agregado por safra | Só o valor atual | Responde "por que o número de março mudou desde a reunião" |
| D14 | Fontes do contrato v2 entram **só como colunas** (`seeds/v2/`, sem linhas); a lógica é provada por unit tests | Inventar linhas de exemplo | Todo número publicado vem do enunciado. Os modelos rodam com a fonte vazia, como em produção antes de o produtor entregar |
| D15 | Ingestão no **Lakeflow Declarative Pipelines** (SQL), com expectations do contrato e quarentena por fonte | Jobs de ingestão escritos em PySpark | Auto Loader e leitura do Kafka com checkpoint, nova tentativa e métricas de qualidade gerenciados pela plataforma, com menos código para manter |
| D16 | PySpark só onde SQL não atende bem: **variáveis de velocidade** e reprocessamento pesado de safras | PySpark em toda a cadeia / só dbt | Janelas de tempo por cliente sobre grande volume. O restante fica em dbt, revisável pelo time de Risco |
| D17 | Orquestração no **Lakeflow Jobs** com quatro gatilhos: 15 min, chegada de arquivo de chargeback, diário e semanal | Airflow | Toda a execução está no Databricks; o gatilho por chegada de arquivo atende ao reprocessamento a cada chargeback |

## Premissas (validar com Risco e com os produtores)

- Timestamps no fuso do país da operação. A safra é a data local. O contrato v2 exige offset.
- Moeda por transação (`currency`); comparações entre países só após conversão pela taxa da data.
- Pix não tem chargeback: fica fora do denominador do CB rate de cartão, mas tem perda própria via MED (Mecanismo Especial de Devolução, do Banco Central).
- O status `error` entra no denominador da aprovação porque o contrato manda. A aprovação por pedido é a métrica auxiliar.
- Limites das bandeiras parametrizados em `dbt_project.yml` (`network_programs`): VAMP lojista 1,5% na América Latina e Caribe, com piso de 1.500 casos de fraude reportada + disputas no mês (Visa, VAMP fact sheet 2025; o Brasil terá programa próprio, ainda não anunciado); Mastercard ECM 1,5% com 100+ chargebacks; EFM 50 bps.

## Perguntas em aberto para o time de Risco

1. **Perda Bruta:** soma de todos os chargebacks (contrato literal) ou só fraude? E o "período" é data de notificação ou safra?
2. **Perda da plataforma:** qual o processo de cobrança de saldo negativo do lojista e qual a taxa de recuperação?
3. **tx_1004 e tx_1005:** reentrega do gateway ou cobrança em dobro (mesmo pedido)? Correção técnica ou de produto?
4. **tx_1005 sem avaliação:** existe fallback que aprova em caso de timeout do motor ou do parceiro antifraude?
5. **Rótulo antecipado:** recebemos TC40/SAFE ou alertas (Ethoca, RDR) da adquirente?
6. **Grupo de controle:** existe amostra aprovada sem regras para medir falso positivo?
7. **Janela de 90 dias:** veio de dado ou de convenção? A curva de maturação responde depois de um trimestre.
8. **Multi-país:** a mesma definição vale para as operações de outros países, com outros meios e prazos de disputa?

## Limiares de alerta (primeira versão, calibrar depois de 4 semanas)

| Sinal | P1 (PagerDuty) | P2 (Slack) |
|---|---|---|
| Aprovadas sem avaliação | > 1% no dia | > 0 |
| Freshness do feed de chargeback / TC40 | > 4 dias | > 2 dias |
| Ratio VAMP / ECM projetado do lojista | acima do limite | acima de 75% do limite |
| Z-score de disparo por regra | — | \|z\| > 3 |
| Z-score de volume e de recusa total | — | \|z\| > 3 |
| PSI do risk_score | > 0,25 | > 0,10 |
| Reconciliação Bronze ↔ Gold | qualquer diferença (bloqueia a publicação) | — |
