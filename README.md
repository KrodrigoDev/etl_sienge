# Estrutura do Pipeline

```
pipeline/
│
├── main.py                          ← orquestrador (extract → transform → load)
│
├── src/
│   └── drivers/
│       └── selenium_requester.py   ← driver Edge + helpers genéricos
│
└── stages/
    ├── extract/
    │   ├── extract_painel_compras.py  ← extrai painel de compras
    │   └── extract_estoque.py         ← extrai estoque de obras
    │
    ├── transform/
    │   ├── input/                     ← CSVs brutos (saída do extract)
    │   │   ├── painel_compras/
    │   │   └── estoque/
    │   ├── output/                    ← tabelas do DW (entrada do Power BI)
    │   └── etl_suprimentos.py         ← (a mover aqui)
    │
    └── load/
        └── (próxima etapa)
```

## Uso

```bash
# Pipeline completo
python main.py

# Só extração
python main.py --etapa extract

# Só transformação (usa CSVs já baixados)
python main.py --etapa transform

# Data de início customizada
python main.py --data-inicio 01/01/2025
```
