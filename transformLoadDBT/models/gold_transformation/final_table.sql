{{ config(materialized='table') }}

WITH bancos_empregados_reclamacoes AS (
    SELECT
        ano,
        trimestre,
        categoria,
        tipo,
        cnpj,
        instuicao_financeira,
        indice,
        quantidade_de_reclamacoes_reguladas_procedentes,
        quantidade_de_reclamacoes_reguladas_outras,
        quantidade_de_reclamações_nao_reguladas,
        quantidade_total_de_reclamacoes,
        quantidade_total_de_clientes_CCS_e_SCR,
        quantidade_de_clientes_CCS,
        quantidade_de_clientes_SCR,
        employer_name,
        reviews_count,
        culture_count,
        salaries_count,
        benefits_count,
        employer_website,
        employer_headquarters,
        employer_founded,
        employer_industry,
        employer_revenue,
        url,
        geral,
        cultura_e_valores,
        diversidade_e_inclusao,
        qualidade_de_vida,
        alta_lideranca,
        remuneracao_e_beneficios,
        oportunidades_de_carreira,
        recomendam_para_outras_pessoas,
        perspectiva_positiva_da_empresa,
        match_percent,
        segmento,
        nome,
        ROW_NUMBER() OVER (PARTITION BY cnpj, ano ORDER BY cnpj, ano) AS row_num
    FROM
        eEDB011.bancos_empregados_reclamacoes
)


SELECT DISTINCT
    *
FROM
    bancos_empregados_reclamacoes
WHERE
    row_num = 1
