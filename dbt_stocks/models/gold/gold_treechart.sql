WITH source AS (
  SELECT
    symbol,
    TRY_CAST(current_price AS DOUBLE) AS current_price_dbl,
    market_timestamp
  FROM {{ ref('silver_clean_stock_quotes') }}
  -- optionally filter invalid rows:
  WHERE TRY_CAST(current_price AS DOUBLE) IS NOT NULL
),

latest_day AS (
  -- if market_timestamp is epoch seconds (NUMBER/INT):
  SELECT CAST(TO_TIMESTAMP_LTZ(MAX(market_timestamp)) AS DATE) AS max_day
  FROM source
),

latest_prices AS (
  SELECT
    symbol,
    AVG(current_price_dbl) AS avg_price
  FROM source
  JOIN latest_day ld
    ON CAST(TO_TIMESTAMP_LTZ(market_timestamp) AS DATE) = ld.max_day
  GROUP BY symbol
),

all_time_volatility AS (
  SELECT
    symbol,
    STDDEV_POP(current_price_dbl) AS volatility,             
    CASE
      WHEN AVG(current_price_dbl) = 0 THEN NULL
      ELSE STDDEV_POP(current_price_dbl) / NULLIF(AVG(current_price_dbl), 0)
    END AS relative_volatility
  FROM source
  GROUP BY symbol
)

SELECT
  lp.symbol,
  lp.avg_price,
  v.volatility,
  v.relative_volatility
FROM latest_prices lp
JOIN all_time_volatility v ON lp.symbol = v.symbol
ORDER BY lp.symbol
