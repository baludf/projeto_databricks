# projeto_databricks

Pipeline lakehouse (Bronze / Silver / Gold + ML + Genie + App) com Databricks Asset Bundles (DAB) para o
case **Rota do Perfume** — aulas de engenharia de dados.

## Estrutura

```
.
├── databricks.yml              # Configuração do DAB (catalog, host, target dev/prod)
├── resources/                  # Recursos do bundle
│   ├── catalogo.yml            # Schemas bronze/silver/gold + Volume bronze.raw
│   ├── pipeline.job.yml        # Job rotaperfume_pipeline (17 tasks)
│   ├── dashboard.dashboard.yml # Dashboard AI/BI no bundle
│   ├── dashboard-comercial.lvdash.json # Dashboard comercial — KPIs, receita, margem
│   ├── direcao.geniespace.json # Genie Space "Rota do Perfume · Direção"
│   └── comercial.geniespace.json # Genie Space "Rota do Perfume · Comercial"
├── scripts/                    # Scripts auxiliares
│   ├── criar-catalogo.sh       # Cria o catálogo lakehouse_rotaperfume
│   ├── subir-raw.sh            # Sobe os 10 CSVs para o Volume
│   └── rodar-tarefa.sh         # Roda uma task isolada do pipeline
├── src/                        # Código do pipeline
│   ├── raw/                    # Conferência dos CSVs no Volume
│   ├── bronze/                 # Ingestão como STRING de 10 tabelas Delta
│   ├── silver/                 # Limpeza, tipagem e constraints CHECK
│   │   ├── 01-clientes.py
│   │   ├── 02-pedidos.py
│   │   ├── 03-itens-e-produtos.py
│   │   └── 04-crm-e-financeiro.py
│   ├── gold/                   # Dimensões, fato, marts, testes, métricas, retorno
│   │   ├── 05-dimensoes.py
│   │   ├── 06-fato-vendas.py
│   │   ├── 07-marts.py
│   │   ├── 08-testes.py
│   │   ├── 09-metricas-negocio.sql
│   │   ├── 10-auditoria-metadado.sql
│   │   ├── 11-retorno-ligacao.sql
│   │   └── 12-dashboard-refresh.py
│   └── ml/                     # Machine Learning
│       ├── 11-features.py      # Feature engineering (20 features, 4 grupos)
│       ├── 12-modelo.py        # Treino HistGradientBoosting + MLflow
│       └── 13-fila.sql         # Fila semanal 200 contatos + 4 funções SQL
├── app/                        # Databricks App (rotaperfume-direcao)
│   ├── server/server.ts        # Express + createWorkspaceClient para INSERT/MERGE
│   ├── client/src/pages/       # SemanaPage, PerguntarPage, AcompanhamentoPage
│   └── config/queries/         # SQL analytics: fila, kpis_semana, vendedores, acompanhamento
└── dados/                      # CSVs de exemplo (erp/ e crm/)
```

## Pipeline (17 tasks)

```
raw_conferencia
  └── bronze_ingestao
        ├── silver_clientes         (paralelo)
        ├── silver_pedidos          (paralelo)
        ├── silver_itens_produtos   (paralelo)
        └── silver_crm_financeiro   (paralelo)
               └── gold_dimensoes
                     └── gold_fato_vendas
                           └── gold_marts
                                 ├── metricas_de_negocio
                                 │     └── auditoria_de_metadado
                                 │           └── testes
                                 │                 └── dashboard_refresh
                                 ├── gold_retorno_ligacao
                                 └── ml_features
                                       └── ml_modelo
                                             └── ml_fila
```

## Camadas

| Camada | Conteúdo | Formato |
|---|---|---|
| **Bronze** | 10 tabelas — dado cru, todas colunas como STRING | Delta, sobrescreve a cada run |
| **Silver** | 10 tabelas — tipos corrigidos, dedup, CHECK constraints | Delta, com contrato via `ALTER TABLE ... CHECK` |
| **Gold** | 4 dimensões, fato_vendas, 3 marts, 9 testes, métricas, retorno_ligacao | Delta |
| **ML** | Features (20), modelo HistGradientBoosting, score_propensao, fila semanal 200 | MLflow + Delta |
| **Genie** | 2 spaces: Comercial (12 fontes) e Direção (7 fontes) | Genie API |
| **App** | Databricks App — KPIs, fila, retorno de ligações, acompanhamento | AppKit + React |
| **Dashboard** | AI/BI comercial — KPIs, receita, margem, filtros cruzados | `.lvdash.json` |

## Genie Spaces

| Space | Audiência | Fontes | Foco |
|---|---|---|---|
| **Rota do Perfume · Comercial** | Time comercial (vendedores) | 12 tabelas | Vendas, clientes, fila, ruptura |
| **Rota do Perfume · Direção** | Direção comercial | 7 tabelas | Decisão de ligação, retorno, lift_top200 |

## Databricks App (rotaperfume-direcao)

App para a direção comercial com 3 páginas:

- **Semana** — KPIs da semana, fila de 200 contatos com score/faixa/motivo, botões de retorno (Vendeu / Vai pensar / Sem interesse / Não atendeu)
- **Perguntar** — Genie Chat embarcado (em desenvolvimento)
- **Acompanhamento** — Status dos retornos por vendedor

Deploy: `databricks apps deploy --profile projeto-dados-ia`

## Como executar

```bash
# 1) Autenticar no workspace
databricks auth login --profile projeto-dados-ia

# 2) Criar o catálogo
bash scripts/criar-catalogo.sh projeto-dados-ia

# 3) Subir os CSVs para o Volume
bash scripts/subir-raw.sh projeto-dados-ia

# 4) Validar / deploy do bundle
databricks bundle validate --profile projeto-dados-ia
databricks bundle deploy  --profile projeto-dados-ia

# 5) Rodar o pipeline (17 tasks)
databricks bundle run rotaperfume_pipeline --profile projeto-dados-ia

# 6) Deploy do App
cd app && databricks apps deploy --profile projeto-dados-ia
```

## Lições aprendidas (e armadilhas)

- **Não usar `mode: development`** no target `dev` — em Free Edition com Default
  Storage isso prefixa os schemas com `dev_<user>_` e quebra todos os SQL.
- **Genie spaces** devem ser criados via `databricks genie create-space` com `version: 2` no `serialized_space`. Os YAMLs `.genie_space.yml` não são reconhecidos pelo bundle CLI.
- **`column_configs`** em Genie spaces devem estar ordenados por `column_name`, e `data_sources.tables` por `identifier`.
- **AppKit `Select`** usa `SelectTrigger`/`SelectContent`/`SelectItem`, não `<option>` HTML.
- **AppKit `Alert`** requer `AlertDescription` para o texto expandir corretamente.
- **`createWorkspaceClient({})`** funciona dentro de `onPluginsReady` para autenticação no Databricks App.
- **`useAnalyticsQuery`** cacheia resultados — use um parâmetro dummy (`w`) para forçar refetch após writes.
- **MERGE** é mais seguro que `INSERT ON CONFLICT` quando a coluna de conflito pode ter valores inconsistentes.
