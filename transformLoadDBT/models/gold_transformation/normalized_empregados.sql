{{ config(materialized='table') }}

WITH raw_empregados AS (
    SELECT 
        employer_name,
        reviews_count,
        culture_count,
        salaries_count,
        benefits_count,
        `employer-website`,
        `employer-headquarters`,
        `employer-founded`,
        `employer-industry`,
        `employer-revenue`,
        url,
        Geral,
        `Cultura e valores`,
        `Diversidade e inclusão`,
        `Qualidade de vida`,
        `Alta liderança`,
        `Remuneração e benefícios`,
        `Oportunidades de carreira`,
        `Recomendam para outras pessoas(%)`,
        `Perspectiva positiva da empresa(%)`,
        Segmento,
        Nome,
        match_percent,
        CNPJ
    FROM
        eEDB011.empregados
),
normalized_empregados AS (
    SELECT
        re.employer_name,
        re.reviews_count,
        re.culture_count,
        re.salaries_count,
        re.benefits_count,
        `employer-website` AS employer_website,
        `employer-headquarters` AS employer_headquarters,
        `employer-founded` AS employer_founded,
        `employer-industry` AS employer_industry,
        `employer-revenue` AS employer_revenue,
        url,
        Geral AS geral,
        `Cultura e valores` AS cultura_e_valores,
        `Diversidade e inclusão` AS diversidade_e_inclusao,
        `Qualidade de vida` AS qualidade_de_vida,
        `Alta liderança` AS alta_lideranca,
        `Remuneração e benefícios` AS remuneracao_e_beneficios,
        `Oportunidades de carreira` AS oportunidades_de_carreira,
        `Recomendam para outras pessoas(%)` AS recomendam_para_outras_pessoas,
        `Perspectiva positiva da empresa(%)` AS perspectiva_positiva_da_empresa,
        Segmento as segmento,
        Nome as nome,
        match_percent,
        CNPJ as cnpj 
    FROM 
        raw_empregados re
) 

SELECT 
    *
FROM
    normalized_empregados