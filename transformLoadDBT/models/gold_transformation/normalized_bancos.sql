
{{ config(materialized='table') }}

with raw_bancos AS (
    SELECT
        Segmento,
        Nome,
        CNPJ
    FROM
        eEDB011.bancos

),
normalized_bancos AS (
    SELECT
        Segmento AS segmento,
        Nome AS nome,
        CNPJ AS cnpj
    FROM raw_bancos rb
)

SELECT *
FROM normalized_bancos
