WITH banks AS (
    SELECT DISTINCT bank_name FROM {{ ref('bank_2022_23') }}
    UNION
    SELECT DISTINCT bank_name FROM {{ ref('bank_2023_24') }}
)

SELECT
    {{ dbt_utils.surrogate_key(['bank_name']) }} AS bank_key,
    bank_name
FROM banks
