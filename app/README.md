# rotaperfume-direcao

Databricks App para a **direção comercial** da Rota do Perfume. Mostra a fila semanal de 200 contatos, permite registrar o retorno das ligações e acompanhar o progresso por vendedor.

## Páginas

| Página | O que faz |
|---|---|
| **Semana** | KPIs (contatos, receita esperada, conversão, trabalhados), filtro por vendedor, tabela da fila com score/faixa/motivo/sugestão, botões de retorno |
| **Perguntar** | Genie Chat embarcado do space "Rota do Perfume · Direção" |
| **Acompanhamento** | Resumo dos retornos registrados: trabalhados, vendeu, vai_pensar, sem_interesse, não atendeu |

## Stack

- **Backend**: Node.js + Express via AppKit `server()` plugin
- **Frontend**: React + TypeScript + Vite + Tailwind CSS
- **UI**: AppKit UI (Card, Table, Select, Badge, Button, Alert, Skeleton, Empty)
- **Dados**: AppKit `analytics()` plugin — queries SQL via SSE
- **Escrita**: `createWorkspaceClient({})` + `statementExecution.executeStatement` para MERGE em `retorno_ligacao`

## Queries SQL

| Arquivo | Parâmetros | Descrição |
|---|---|---|
| `config/queries/kpis_semana.sql` | `vendedor`, `w` | KPIs agregados da semana |
| `config/queries/fila.sql` | `vendedor`, `w` | Fila de 200 contatos com retorno |
| `config/queries/vendedores.sql` | — | Lista de vendedores com contagem |
| `config/queries/acompanhamento.sql` | — | Retornos por vendedor |

O parâmetro `w` (writeKey) é um dummy para forçar refetch do `useAnalyticsQuery` após gravações.

## Deploy

```bash
databricks apps deploy --profile projeto-dados-ia
```

O app fica em: `https://rotaperfume-direcao-7474645455933146.aws.databricksapps.com`

## Estrutura

```
app/
├── server/server.ts            # Express + rotas /api/retorno, /api/quem-sou, /api/test
├── client/
│   ├── src/
│   │   ├── App.tsx             # Layout + react-router (Semana, Perguntar, Acompanhamento)
│   │   └── pages/
│   │       ├── SemanaPage.tsx   # KPIs + fila + RetornoButtons
│   │       ├── PerguntarPage.tsx
│   │       └── AcompanhamentoPage.tsx
│   └── index.html
├── config/queries/             # SQL analytics
├── databricks.yml              # Bundle config
├── app.yaml                    # App config (command + env)
└── package.json                # AppKit 0.57.0
```
