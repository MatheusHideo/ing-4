{{ config(materialized='table') }}

WITH raw_empregados AS (
    SELECT 
        employer_name,
        reviews_count,
        culture_count,
        salaries_count,
        benefits_count,
        "employer-website",
        "employer-headquarters",
        "employer-founded",
        "employer-industry",
        "employer-revenue",
        "url",
        "Geral",
        "Cultura e valores",
        "Diversidade e inclusão",
        "Qualidade de vida",
        "Alta liderança",
        "Remuneração e benefícios",
        "Oportunidades de carreira",
        "Recomendam para outras pessoas(%)",
        "Perspectiva positiva da empresa(%)",
        "Segmento",
        "Nome",
        "match_percent",
        "CNPJ"
    FROM
        eedb011.empregados
),
normalized_empregados AS (
    SELECT
        re.employer_name:: character varying,
        re.reviews_count:: bigint,
        re.culture_count:: bigint,
        re.salaries_count:: bigint,
        re.benefits_count:: bigint,
        "employer-website":: character varying AS employer_website,
        "employer-headquarters":: character varying AS employer_headquarters,
        "employer-founded":: bigint AS employer_founded,
        "employer-industry":: character varying AS employer_industry,
        "employer-revenue":: character varying AS employer_revenue,
        "url":: character varying,
        "Geral":: double precision AS geral,
        "Cultura e valores":: double precision AS cultura_e_valores,
        "Diversidade e inclusão":: double precision AS diversidade_e_inclusao,
        "Qualidade de vida":: double precision AS qualidade_de_vida,
        "Alta liderança":: double precision AS alta_lideranca,
        "Remuneração e benefícios":: double precision AS remuneracao_e_beneficios,
        "Oportunidades de carreira":: double precision AS oportunidades_de_carreira,
        "Recomendam para outras pessoas(%)":: double precision AS recomendam_para_outras_pessoas,
        "Perspectiva positiva da empresa(%)":: double precision AS perspectiva_positiva_da_empresa,
        "Segmento":: character varying as segmento,
        "Nome":: character varying as nome,
        match_percent:: bigint,
        "CNPJ":: character varying as cnpj 
    FROM 
        raw_empregados re
) 

SELECT 
    *
FROM
    normalized_empregados