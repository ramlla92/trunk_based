WITH base AS (
    SELECT
      bank_key,
      year,
      profit_for_year,
      total_asset,
      paid_up_capital
    FROM {{ ref('fct_bank_performance') }}
),

growth AS (
    SELECT
      curr.bank_key,
      curr.year,
      curr.profit_for_year,
      prev.profit_for_year AS profit_for_year_prev,
      (curr.profit_for_year - prev.profit_for_year) AS profit_growth,
      CASE WHEN prev.profit_for_year = 0 THEN NULL ELSE ((curr.profit_for_year - prev.profit_for_year) / prev.profit_for_year) * 100 END AS profit_growth_pct,

      curr.total_asset,
      prev.total_asset AS total_asset_prev,
      (curr.total_asset - prev.total_asset) AS total_asset_growth,
      CASE WHEN prev.total_asset = 0 THEN NULL ELSE ((curr.total_asset - prev.total_asset) / prev.total_asset) * 100 END AS total_asset_growth_pct

    FROM base curr
    LEFT JOIN base prev ON curr.bank_key = prev.bank_key AND prev.year = CASE curr.year WHEN '2023/24' THEN '2022/23' ELSE NULL END
)

SELECT * FROM growth
