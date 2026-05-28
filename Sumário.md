|        Taxonomy Quadrant|    Subreddits|    Messages|    Unique Users|
| :-----------------------| -----------: | ---------: | -------------: |
|           Hostile Echoes|             3|      192005|           45319|
|         Chronic Conflict|             7|      388030|           52181|
|      Passive Consumption|             7|      365218|           68444|
|Constructive Deliberation|             3|      154983|           34893|
|                    TOTAL|            20|     1100236|          200837|


# --- ANÁLISE: SENTIMENTO POSITIVE ---
Kruskal-Wallis H: 14.2381 | p-value: 2.5983e-03
Resultado: Diferença global significativa encontrada. Executando Teste de Dunn (Post-Hoc)...

### Matriz de p-values (Dunn's Test - Bonferroni):
|                                |Hostile Echoes   |Chronic Conflict |Passive Consumption |Constructive Deliberation|
| :------------------------------|---------------: |---------------: |------------------: |-----------------------: |
|Hostile Echoes                  |1.0000           |0.6148           |1.0000              |1.0000                   |
|Chronic Conflict                |0.6148           |1.0000           |0.0030*             |0.0423*                  |
|Passive Consumption             |1.0000           |0.0030*          |1.0000              |1.0000                   |
|Constructive Deliberation       |1.0000           |0.0423*          |1.0000              |1.0000                   |

* DICA: Valores com '*' (< 0.05) indicam que a diferença entre os dois cruzamentos é estatisticamente significativa.
------------------------------------------------------------
# --- ANÁLISE: SENTIMENTO NEUTRAL ---
Kruskal-Wallis H: 12.4748 | p-value: 5.9216e-03
Resultado: Diferença global significativa encontrada. Executando Teste de Dunn (Post-Hoc)...

### Matriz de p-values (Dunn's Test - Bonferroni):
|                                |Hostile Echoes   |Chronic Conflict |Passive Consumption |Constructive Deliberation|
| :------------------------------|---------------: |---------------: |------------------: |-----------------------: |
|Hostile Echoes                  | 1.0000          | 1.0000          |    1.0000          |          0.6749         |
|Chronic Conflict                | 1.0000          | 1.0000          |    0.0199*         |          0.0229*        |
|Passive Consumption             | 1.0000          | 0.0199*         |    1.0000          |          1.0000         |
|Constructive Deliberation       | 0.6749          | 0.0229*         |    1.0000          |          1.0000         |
* DICA: Valores com '*' (< 0.05) indicam que a diferença entre os dois cruzamentos é estatisticamente significativa.
------------------------------------------------------------

# --- ANÁLISE: SENTIMENTO NEGATIVE ---
Kruskal-Wallis H: 15.4354 | p-value: 1.4800e-03
Resultado: Diferença global significativa encontrada. Executando Teste de Dunn (Post-Hoc)...

### Matriz de p-values (Dunn's Test - Bonferroni):
|                                |Hostile Echoes   |Chronic Conflict |Passive Consumption |Constructive Deliberation|
| :------------------------------|---------------: |---------------: |------------------: |-----------------------: |
|Hostile Echoes                  | 1.0000          | 1.0000          |    0.6148          |          0.5070         |
|Chronic Conflict                | 1.0000          | 1.0000          |    0.0042*         |          0.0145*        |
|Passive Consumption             | 0.6148          | 0.0042*         |    1.0000          |          1.0000         |
|Constructive Deliberation       | 0.5070          | 0.0145*         |    1.0000          |          1.0000         |
* DICA: Valores com '*' (< 0.05) indicam que a diferença entre os dois cruzamentos é estatisticamente significativa.

============================================================
### CÁLCULO DO DELTA DE COMPREENSÃO MULTIMODAL 

Total de interações comparadas: 1,071,383
Total de divergências (IA mudou de ideias): 14,532
Taxa de Impacto da Visão (Delta Global): 1.36%

------------------------------------------------------------
### MATRIZ DE TRANSIÇÃO (Cego -> Com Visão)

Quando a IA Cega disse POSITIVE, com visão ela percebeu que era:
  -> POSITIVE: 105,581 vezes
  -> NEUTRAL: 581 vezes
  -> NEGATIVE: 66 vezes

Quando a IA Cega disse NEUTRAL, com visão ela percebeu que era:
  -> POSITIVE: 1,918 vezes
  -> NEUTRAL: 382,731 vezes
  -> NEGATIVE: 9,427 vezes

Quando a IA Cega disse NEGATIVE, com visão ela percebeu que era:
  -> POSITIVE: 55 vezes
  -> NEUTRAL: 2,485 vezes
  -> NEGATIVE: 568,539 vezes

------------------------------------------------------------
### DEPENDÊNCIA VISUAL POR SUBREDDIT 
|Subreddit            | Variação                     |
| :-------------------|----------------------------: |
|r/ShitpostBR         | Delta: 6.19% (3,396 mudanças)|
|r/MemesBR            | Delta: 4.33% (2,122 mudanças)|
|r/videogamesbrasil   | Delta: 2.18% (850 mudanças)|
|r/InfernoSocial      | Delta: 1.36% (995 mudanças)|
|r/gamesEcultura      | Delta: 1.32% (648 mudanças)|
|r/brasilivre         | Delta: 1.32% (509 mudanças)|
|r/botecodoreddit     | Delta: 1.23% (529 mudanças)|
|r/computadores       | Delta: 1.17% (564 mudanças)|
|r/opiniaoimpopular   | Delta: 1.13% (1,014 mudanças)|
|r/brasil             | Delta: 1.05% (707 mudanças)|
|r/farialimabets      | Delta: 1.05% (728 mudanças)|
|r/FilosofiaBAR       | Delta: 1.01% (586 mudanças)|
|r/carros             | Delta: 0.75% (461 mudanças)|
|r/DebatesBr          | Delta: 0.65% (324 mudanças)|
|r/saopaulo           | Delta: 0.57% (239 mudanças)|
|r/OpiniaoBurra       | Delta: 0.52% (390 mudanças)|
|r/NoticiasBR         | Delta: 0.49% (300 mudanças)|
|r/antitrampo         | Delta: 0.41% (125 mudanças)|
|r/BrasildoB          | Delta: 0.07% (15 mudanças)|
|r/futebol            | Delta: 0.06% (30 mudanças)|
