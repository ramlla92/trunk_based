WITH base_2022 AS (
    SELECT
        {{ dbt_utils.surrogate_key(['s.bank_name', '2022/23']) }} AS bank_performance_key,
        b.bank_key,
        d.date_key,
        '2022/23' AS year,
        s.total_asset,
        s.total_asset_usd,
        s.paid_up_capital,
        s.paid_up_capital_usd,
        s.total_capital,
        s.total_capital_usd,
        s.profit_before_tax,
        s.profit_before_tax_usd,
        s.net_income,
        s.net_income_usd,
        s.profit_for_the_year AS profit_for_year,
        s.profit_for_the_year_usd,
        s.roe,
        s.eps,
        s.roa,
        s.bank_name AS bank_name,  -- explicitly from source

        CASE
            WHEN s.total_capital < 2000000000 THEN 1
            WHEN s.total_capital < 5000000000 THEN 2
            ELSE 3
        END AS capital_tier_id,

        CASE
            WHEN s.total_asset < 20000000000 THEN 1
            WHEN s.total_asset < 50000000000 THEN 2
            ELSE 3
        END AS asset_size_id,

        CASE
            WHEN s.profit_for_the_year < 0 THEN 5
            WHEN s.profit_for_the_year < 500000000 THEN 4
            WHEN s.profit_for_the_year < 2000000000 THEN 3
            WHEN s.profit_for_the_year < 5000000000 THEN 2
            ELSE 1
        END AS profit_band_id

    FROM {{ ref('bank_2022_23') }} AS s
    LEFT JOIN {{ ref('dim_bank') }} AS b ON s.bank_name = b.bank_name
    LEFT JOIN {{ ref('dim_date') }} AS d ON d.year = '2022/23'
),

base_2023 AS (
    SELECT
        {{ dbt_utils.surrogate_key(['s.bank_name', '2023/24']) }} AS bank_performance_key,
        b.bank_key,
        d.date_key,
        '2023/24' AS year,
        s.total_asset,
        s.total_asset_usd,
        s.paid_up_capital,
        s.paid_up_capital_usd,
        s.total_capital,
        s.total_capital_usd,
        s.profit_before_tax,
        s.profit_before_tax_usd,
        s.net_income,
        s.net_income_usd,
        s.profit_for_the_year AS profit_for_year,
        s.profit_for_the_year_usd,
        s.roe,
        s.eps,
        s.roa,
        s.bank_name AS bank_name,  -- explicitly from source

        CASE
            WHEN s.total_capital < 2000000000 THEN 1
            WHEN s.total_capital < 5000000000 THEN 2
            ELSE 3
        END AS capital_tier_id,

        CASE
            WHEN s.total_asset < 20000000000 THEN 1
            WHEN s.total_asset < 50000000000 THEN 2
            ELSE 3
        END AS asset_size_id,

        CASE
            WHEN s.profit_for_the_year < 0 THEN 5
            WHEN s.profit_for_the_year < 500000000 THEN 4
            WHEN s.profit_for_the_year < 2000000000 THEN 3
            WHEN s.profit_for_the_year < 5000000000 THEN 2
            ELSE 1
        END AS profit_band_id

    FROM {{ ref('bank_2023_24') }} AS s
    LEFT JOIN {{ ref('dim_bank') }} AS b ON s.bank_name = b.bank_name
    LEFT JOIN {{ ref('dim_date') }} AS d ON d.year = '2023/24'
)

SELECT * FROM base_2022
UNION ALL
SELECT * FROM base_2023
