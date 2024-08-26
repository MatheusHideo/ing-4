{{ config(materialized='table', alias='bancos_empregados') }}


WITH normalized_bancos as (
  SELECT
		"segmento",
        "nome",
        "cnpj"
    FROM eedb011.normalized_bancos
),
normalized_empregados AS (
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
        "url",
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
        eedb011.normalized_empregados
)

SELECT 
    e.employer_name,
    e.reviews_count,
    e.culture_count,
    e.salaries_count,
    e.benefits_count,
    e.employer_website,
    e.employer_headquarters,
    e.employer_founded,
    e.employer_industry,
    e.employer_revenue,
    e."url",
    e.geral,
    e.cultura_e_valores,
    e.diversidade_e_inclusao,
    e.qualidade_de_vida,
    e.alta_lideranca,
    e.remuneracao_e_beneficios,
    e.oportunidades_de_carreira,
    e.recomendam_para_outras_pessoas,
    e.perspectiva_positiva_da_empresa,
    e.match_percent,
    b.segmento,
    b.nome,
    b.cnpj
FROM 
    normalized_bancos b
LEFT JOIN
    normalized_empregados e
ON b.cnpj = e.cnpj

