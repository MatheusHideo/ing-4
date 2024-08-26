{{ config(materialized='table') }}

WITH bancos_empregados AS (
      SELECT
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
        e.`url`,
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
    	cnpj
    FROM 
        eEDB011.bancos_empregados e
),
reclamacoes AS (
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
        quantidade_de_clientes_SCR
    FROM 
        eEDB011.normalized_reclamacoes
)

SELECT DISTINCT
    r.ano,
    r.trimestre,
    r.categoria,
    r.tipo,
    r.cnpj,
    r.instuicao_financeira,
    r.indice,
    r.quantidade_de_reclamacoes_reguladas_procedentes,
    r.quantidade_de_reclamacoes_reguladas_outras,
    r.quantidade_de_reclamações_nao_reguladas,
    r.quantidade_total_de_reclamacoes,
    r.quantidade_total_de_clientes_CCS_e_SCR,
    r.quantidade_de_clientes_CCS,
    r.quantidade_de_clientes_SCR,
    be.employer_name,
    be.reviews_count,
    be.culture_count,
    be.salaries_count,
    be.benefits_count,
    be.employer_website,
    be.employer_headquarters,
    be.employer_founded,
    be.employer_industry,
    be.employer_revenue,
    be.`url`,
    be.geral,
    be.cultura_e_valores,
    be.diversidade_e_inclusao,
    be.qualidade_de_vida,
    be.alta_lideranca,
    be.remuneracao_e_beneficios,
    be.oportunidades_de_carreira,
    be.recomendam_para_outras_pessoas,
    be.perspectiva_positiva_da_empresa,
    be.match_percent,
    be.segmento,
    be.nome
FROM
    bancos_empregados be
INNER JOIN
    reclamacoes r
ON
    be.cnpj = r.cnpj
AND 
    be.nome = r.instuicao_financeira
