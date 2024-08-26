
{{ config(materialized='table') }}

with raw_bancos AS (
    SELECT
        "Segmento",
        "Nome",
        "CNPJ"
    FROM
        eedb011.bancos

),
normalized_bancos AS (
    SELECT
        "Segmento":: character varying AS segmento,
        "Nome":: character varying AS nome,
        "CNPJ":: character varying AS cnpj
    FROM raw_bancos rb
)

SELECT *
FROM normalized_bancos
