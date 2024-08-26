{{ config(materialized='table', alias='normalized_reclamacoes') }}

WITH raw_reclamacoes AS (
    SELECT 
        Ano,
        Trimestre,
        Categoria,
        Tipo,
        CNPJ,
        `Instituição financeira`,
        `Índice`,
        `Quantidade de reclamações reguladas procedentes`,
        `Quantidade de reclamações reguladas - outras`,
        `Quantidade de reclamações não reguladas`,
        `Quantidade total de reclamações`,
        `Quantidade total de clientes – CCS e SCR`,
        `Quantidade de clientes – CCS`,
        `Quantidade de clientes – SCR`
    FROM
        eEDB011.reclamacoes
),
normalized_reclamacoes AS (
    SELECT
        Ano AS ano,
        Trimestre AS trimestre,
        Categoria AS categoria,
        Tipo AS tipo,
        CNPJ AS cnpj,
        `Instituição financeira` AS instuicao_financeira,
        `Índice` AS indice,
        `Quantidade de reclamações reguladas procedentes` AS quantidade_de_reclamacoes_reguladas_procedentes,
        `Quantidade de reclamações reguladas - outras` AS quantidade_de_reclamacoes_reguladas_outras,
        `Quantidade de reclamações não reguladas` AS quantidade_de_reclamações_nao_reguladas,
        `Quantidade total de reclamações` AS quantidade_total_de_reclamacoes,
        `Quantidade total de clientes – CCS e SCR` AS quantidade_total_de_clientes_CCS_e_SCR,
        `Quantidade de clientes – CCS` AS quantidade_de_clientes_CCS,
        `Quantidade de clientes – SCR` AS quantidade_de_clientes_SCR 
    FROM 
        raw_reclamacoes re
) 

SELECT 
    *
FROM
    normalized_reclamacoes