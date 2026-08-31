# projeto_databricks

Pipeline lakehouse (Bronze / Silver / Gold) com Databricks Asset Bundles (DAB) para o
case **Rota Perfume** — aulas de engenharia de dados.

## Estrutura

```
.
├── databricks.yml          # Configuração do DAB (catalog, host, target dev/prod)
├── pyproject.toml          # Metadado do pacote (sem build wheel por enquanto)
├── resources/              # Recursos do bundle (schemas, volume, jobs)
│   ├── catalogo.yml        # Schemas bronze/silver/gold + Volume bronze.raw
│   └── pipeline.job.yml    # Job rotaperfume_pipeline (raw → bronze → silver)
├── scripts/                # Scripts auxiliares invocados via databricks CLI
│   ├── criar-catalogo.sh   # Cria o catálogo lakehouse_rotaperfume (SQL, fora do bundle)
│   └── subir-raw.sh        # Sobe os 10 CSVs do dados/ para o Volume /Volumes/.../raw
├── src/                    # Notebooks PySpark (serverless)
│   ├── raw/                # Conferência dos CSVs no Volume
│   ├── bronze/             # Ingestão como STRING de 10 tabelas Delta
│   └── silver/             # Limpeza, tipagem e constraints CHECK
│       ├── 01-clientes.py
│       ├── 02-pedidos.py
│       ├── 03-itens-e-produtos.py
│       └── 04-crm-e-financeiro.py
└── dados/                  # CSVs de exemplo (erp/ e crm/)
    ├── erp/                # 5 CSVs de vendas (clientes, pedidos, itens, produtos, pagamentos, estoque)
    └── crm/                # 5 CSVs de CRM (carteira, oportunidades, vendedores, visitas, clientes)
```

## Como executar

```bash
# 1) Autenticar no workspace
databricks auth login --profile projeto-dados-ia

# 2) Criar o catálogo (SQL — fora do bundle por causa do Free Edition)
bash scripts/criar-catalogo.sh projeto-dados-ia

# 3) Subir os CSVs para o Volume
bash scripts/subir-raw.sh projeto-dados-ia

# 4) Validar / fazer deploy do bundle
databricks bundle validate --profile projeto-dados-ia
databricks bundle deploy  --profile projeto-dados-ia

# 5) Rodar o pipeline
databricks bundle run rotaperfume_pipeline --profile projeto-dados-ia
```

## Camadas

| Camada | Conteúdo | Formato |
|---|---|---|
| **Bronze** | 10 tabelas — dado cru, todas colunas como STRING | Delta, sobrescreve a cada run |
| **Silver** | 10 tabelas — tipos corrigidos, dedup, CHECK constraints | Delta, com contrato via `ALTER TABLE ... CHECK` |
| **Gold** | _em construção_ | métricas para BI |

## Lições aprendidas (e armadilhas)

- **Não usar `mode: development`** no target `dev` — em Free Edition com Default
  Storage isso prefixa os schemas com `dev_<user>_` e quebra todos os SQL.
- **`databricks experimental aitools tools query`** usa SQL posicional, sem `--sql`.
- **Em notebooks serverless** o `spark.sparkContext` não existe — use `dbutils.fs.ls`
  para inspecionar o Volume.
- **ANSI SQL** exige `try_to_date(...)` em vez de `to_date(...)` para tolerar nulos.
- **Constraints CHECK** precisam ser `DROP IF EXISTS` + `ADD` para serem idempotentes.
