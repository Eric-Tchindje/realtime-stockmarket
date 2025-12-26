SELECT
    symbol,
    current_price,
    ROUND(day_high, 2) AS day_high,
    ROUND(day_low, 2) AS day_low,
    ROUND(day_open, 2) AS day_open,
    ROUND(prev_close, 2) AS prev_close,
    change_amount,
    ROUND(change_percent, 4) AS change_percent,
    market_timestamp,
    fetched_at
FROM {{ ref('bronze_stg_stock_quotes') }}
WHERE current_price IS NOT NULL
