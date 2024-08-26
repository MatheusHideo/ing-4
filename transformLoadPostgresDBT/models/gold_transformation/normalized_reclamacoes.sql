{{ config(materialized='table') }}

WITH raw_reclamacoes AS (
    SELECT 
        "Ano",
        "Trimestre",
        "Categoria",
        "Tipo",
        "CNPJ",
        "Instituição financeira",
        "Índice",
        "Quantidade de reclamações reguladas procedentes",
        "Quantidade de reclamações reguladas - outras",
        "Quantidade de reclamações não reguladas",
        "Quantidade total de reclamações",
        "Quantidade total de clientes – CCS e SCR",
        "Quantidade de clientes – CCS",
        "Quantidade de clientes – SCR"
    FROM
        eedb011.reclamacoes
),
normalized_reclamacoes AS (
    SELECT
        "Ano":: bigint AS ano,
        "Trimestre":: character varying AS trimestre,
        "Categoria":: character varying AS categoria,
        "Tipo":: character varying AS tipo,
        "CNPJ":: character varying AS cnpj,
        "Instituição financeira":: character varying AS instuicao_financeira,
        "Índice":: character varying AS indice,
        "Quantidade de reclamações reguladas procedentes":: bigint AS quantidade_de_reclamacoes_reguladas_procedentes,
        "Quantidade de reclamações reguladas - outras":: bigint AS quantidade_de_reclamacoes_reguladas_outras,
        "Quantidade de reclamações não reguladas":: bigint AS quantidade_de_reclamações_nao_reguladas,
        "Quantidade total de reclamações":: bigint AS quantidade_total_de_reclamacoes,
        "Quantidade total de clientes – CCS e SCR":: bigint AS quantidade_total_de_clientes_CCS_e_SCR,
        "Quantidade de clientes – CCS":: bigint AS quantidade_de_clientes_CCS,
        "Quantidade de clientes – SCR":: bigint AS quantidade_de_clientes_SCR 
    FROM 
        raw_reclamacoes re
) 

SELECT 
    *
FROM
    normalized_reclamacoes