# ETL Suprimentos — SIENGE

Pipeline de extração, transformação e carga dos dados de suprimentos do SIENGE para o Power BI.

---

## Estrutura do Projeto

```
etl_sienge/
│
├── main.py                          ← orquestrador (extract → transform → load)
│
├── src/
│   └── drivers/
│       └── selenium_requester.py   ← driver Edge + helpers genéricos reutilizados por todos os extractors
│
└── stages/
    ├── extract/
    │   ├── reference/
    │   │   ├── dim_empresa.csv        ← lista de empresas para o loop do adiantamento
    │   │   └── obras_estoque.csv      ← lista de obras para o filtro do estoque
    │   │
    │   ├── extract_painel_compras.py  ← extrai painel de compras
    │   ├── extract_estoque.py         ← extrai estoque de obras
    │   ├── extract_servico.py         ← extrai solicitações de serviços
    │   ├── extract_contrato.py        ← extrai contratos
    │   └── extract_adiantamento.py   ← extrai adiantamentos (loop por empresa, download XLSX)
    │
    ├── transform/
    │   ├── input/                     ← arquivos brutos (saída do extract)
    │   │   ├── painel_compras/
    │   │   ├── estoque/
    │   │   ├── servico/
    │   │   ├── contrato/
    │   │   └── adiantamento/
    │   │
    │   ├── output/                    ← tabelas do DW (entrada do Power BI)
    │   │
    │   ├── transform_painel_compras.py  ← gera dims base + fato_solicitacao_item
    │   ├── transform_estoque.py         ← expande dims + gera fato_estoque
    │   ├── transform_servico.py         ← gera fato_servico
    │   ├── transform_contratos.py       ← gera fato_contrato
    │   └── transform_adiantamento.py   ← parse hierárquico + gera fato_adiantamento
    │
    └── load/
        └── (próxima etapa)
```

---

## Fluxo do Pipeline

```
extract_painel_compras  ─┐
extract_estoque         ─┤
extract_servico         ─┼─► transform_painel_compras  (dims base)
extract_contrato        ─┤       ↓
extract_adiantamento    ─┘   transform_estoque
                             transform_servico          (dependem das dims base)
                             transform_contrato
                             transform_adiantamento
                                 ↓
                             output/ → Power BI
```

> **Atenção:** `transform_painel_compras` deve sempre rodar primeiro — ele gera
> `dim_obra`, `dim_insumo` e `dim_grupo_insumo` que os demais transforms dependem.

---

## Uso

```bash
# Pipeline completo (extract + transform)
python main.py

# Só extração (todos os módulos em ordem)
python main.py --etapa extract

# Só transformação (usa arquivos já baixados)
python main.py --etapa transform

# Módulo individual — extract
python main.py --etapa extract_painel_compras
python main.py --etapa extract_estoque
python main.py --etapa extract_servico
python main.py --etapa extract_contrato
python main.py --etapa extract_adiantamento

# Módulo individual — transform
python main.py --etapa transform_painel_compras
python main.py --etapa transform_estoque
python main.py --etapa transform_servico
python main.py --etapa transform_contrato
python main.py --etapa transform_adiantamento

# Data de início customizada (padrão: 01/01/ano_atual)
python main.py --data-inicio 01/01/2024
```

---

## Detalhes por Módulo

### extract_adiantamento
- Itera sobre todas as empresas de `dim_empresa.csv`
- Para cada empresa, clica em **Visualizar**, detecta a nova aba de download e move o XLSX para `input/adiantamento/`
- Trata dois tipos de "sem dados": alert nativo do browser e `div.spwAlertaAviso`
- Arquivos gerados: `relatorio - {cod_empresa}.xlsx`

### transform_adiantamento
- Parse hierárquico: propaga `empresa`, `credor` e `documento_vinculado` por estado linha a linha
- Colunas finais: `empresa_cod`, `empresa`, `cod_credor`, `credor`, `documento_vinculado`, `data`, `vencto`, `documento`, `tipo_do_mov`, `vl_movimento`, `saldo`, `observacao`, `nome_arquivo`
- Saída: `fato_adiantamento.csv`

### selenium_requester
Helpers genéricos reutilizados por todos os extractors:

| Método | Descrição |
|---|---|
| `navegacao_inicial` | Login e seleção de perfil |
| `aguardar_e_clicar` | Espera elemento clicável e clica |
| `preencher_campo` | Limpa e preenche campo de texto |
| `aguardar_download` | Aguarda arquivo aparecer na pasta de downloads |
| `exportar_csv_modal` | Sequência padrão do modal de exportação |
| `selecionar_todas_colunas` | Abre seletor de colunas MUI e marca todas |
| `aguardar_carregamento_tabela` | Aguarda spinner do MuiDataGrid sumir |
| `fechar_popup_novidade` | Fecha o MuiDialog de "novidade" do SIENGE quando presente |
| `scrollar_pagina` | Scroll no container principal para revelar elementos |

---

## Pré-requisitos

- Python 3.12+
- Microsoft Edge + EdgeDriver compatível
- Perfil Edge com sessão do SIENGE salva em `C:\SeleniumPerfil\Edge`
- Dependências: `selenium`, `pandas`, `openpyxl`

```bash
pip install -r requirements.txt
```

---

## Logs

Toda execução gera log simultâneo no terminal e em `pipeline.log` na raiz do projeto.

```
2026-04-18 10:20:17 [INFO] ═══ EXTRACT — painel_compras ══...
2026-04-18 10:21:01 [INFO] ▶ Empresa: 46
2026-04-18 10:21:03 [INFO]   Salvo: input/adiantamento/relatorio - 46.xlsx
```