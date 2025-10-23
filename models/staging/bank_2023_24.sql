SELECT
    "S/N"::INT AS sn,
    BANKS AS bank_name,
    '2023/24' AS fiscal_year,
    REPLACE(TOTAL_ASSET, ',', '')::NUMBER AS total_asset,
    REPLACE("Total Asset in 'USD'", ',', '')::NUMBER AS total_asset_usd,
    REPLACE("Paid-up Capital", ',', '')::NUMBER AS paid_up_capital,
    REPLACE("Paid-up Capital in 'USD'", ',', '')::NUMBER AS paid_up_capital_usd,
    REPLACE(TOTAL_CAPITAL, ',', '')::NUMBER AS total_capital,
    REPLACE("Total Capital in 'USD'", ',', '')::NUMBER AS total_capital_usd,
    REPLACE(PROFIT_BEFORE_TAX, ',', '')::NUMBER AS profit_before_tax,
    REPLACE("Profit before tax in 'USD'", ',', '')::NUMBER AS profit_before_tax_usd,

    -- Use comprehensive_income as net_income for 23/24 data
    REPLACE(COMPREHENSIVE_INCOME_FOR_YEAR, ',', '')::NUMBER AS net_income,
    REPLACE("comprehensive income for year in 'USD'", ',', '')::NUMBER AS net_income_usd,

    REPLACE(PROFIT_FOR_THE_YEAR, ',', '')::NUMBER AS profit_for_the_year,
    REPLACE("Profit for the year in 'USD'", ',', '')::NUMBER AS profit_for_the_year_usd,

    {{ convert_percent_to_decimal('ROE') }} AS roe,
    {{ convert_percent_to_decimal('EPS') }} AS eps,
    {{ convert_percent_to_decimal('ROA') }} AS roa
FROM {{ source('Banks', 'Banks_2023_24') }} AS s
