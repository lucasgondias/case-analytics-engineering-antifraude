# Decisões, premissas e perguntas em aberto

## Decisões de modelagem

| # | Decisão | Alternativa descartada | Por quê |
|---|---|---|---|
| D1 | Flags de qualidade **sinalizam** e nunca filtram linhas | Remover duplicatas e transações sem avaliação no staging | Remover em silêncio é o próprio "erro silencioso". A métrica oficial segue o contrato e as flags permitem auditar e recalcular |
| D2 | Incremental por **chaves afetadas** (watermark de `_ingested_at` por fonte) | Reprocessar sempre os últimos 90 dias | Chargeback de D+120 existe (Visa e Mastercard permitem até 120 dias). Janela fixa perde esse evento e reprocessa o que não mudou |
| D3 | Agregados guardam só **numeradores e denominadores** | Gravar taxas prontas | Média de taxas está errada quando o volume varia entre dias. A taxa é razão de somas, calculada na camada semântica |
| D4 | Decisão de risco = **última avaliação até a autorização** (+5s de tolerância) | Última avaliação de todas | Uma nova avaliação depois da autorização não decidiu nada e não pode receber o mérito nem a culpa da decisão |
| D5 | `decline_source` derivado (motor, emissor, erro técnico, desconhecido) | Usar só `status` | Sem isso não dá para medir o impacto das regras na conversão, que é um dos 3 objetivos do case |
| D6 | `decision_source` separa a decisão automática da revisão manual | Usar `rule_triggered` como veio | `rule_manual_review_pass` é desfecho humano. Misturar infla a performance do motor |
| D7 | Classificar chargeback por reason code da bandeira (tabela de referência) | Considerar todo chargeback como fraude | Desacordo comercial (Visa 13.x) não é fraude e distorce a performance do motor |

## Premissas (validar com Risco e com os produtores)

- Timestamps em America/Sao_Paulo. A safra é a data local. O contrato v2 exige offset.
- Moeda BRL implícita. O contrato v2 adiciona `currency`.
- Pix não tem chargeback (o mecanismo é o MED do Bacen), então não entra no denominador do CB rate de cartão.
- O status `error` entra no denominador da aprovação porque o contrato manda. A versão sem erros fica como métrica auxiliar.

## Perguntas em aberto para o time de Risco

1. **Perda Bruta:** é a soma de todos os chargebacks (contrato literal) ou só os de fraude? Proposta: publicar as duas, com a de fraude como headline.
2. **Perda Líquida:** existe feed de reversão (disputa ganha)? Sem ele não há perda líquida nem taxa de vitória em disputa.
3. **tx_1004 e tx_1005:** é reentrega do gateway ou cobrança em dobro real? A resposta define se a correção é técnica (deduplicar) ou de produto (estorno ao cliente).
4. **tx_1005 sem avaliação:** o motor tem fallback por timeout que aprova? Precisa virar `decision_source = fallback_timeout` explícito no contrato.
5. **Grupo de controle:** existe amostra aprovada sem passar pelas regras? Sem ela, falso positivo (cliente bom recusado) não é mensurável.
6. **Janela de 90 dias:** veio de dado ou de regra de bolso? A curva `mon_chargeback_maturation_curve` responde com dado depois de 1 trimestre.

## Limiares de alerta (primeira versão, calibrar depois de 4 semanas)

| Sinal | P1 (PagerDuty) | P2 (Slack) |
|---|---|---|
| Aprovadas sem avaliação | > 1% no dia | > 0 |
| Freshness do feed de chargeback | > 4 dias | > 2 dias |
| Z-score da taxa de recusa do motor | — | \|z\| > 3 |
| Z-score de volume | — | \|z\| > 3 |
| Reconciliação Bronze ↔ Gold | qualquer diferença (bloqueia a publicação) | — |
| PSI do risk_score | > 0,25 | > 0,10 |
