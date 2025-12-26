with enriched as (
    select
        symbol,
        cast(market_timestamp as date) as trade_date,
        day_low,
        day_high,
        current_price,
        first_value(current_price) over (
            partition by symbol, cast(market_timestamp as date)
            order by market_timestamp
        ) as candle_open,
        last_value(current_price) over (
            partition by symbol, cast(market_timestamp as date)
            order by market_timestamp
            rows between unbounded preceding and unbounded following
        ) as candle_close
    from {{ ref('silver_clean_stock_quotes') }}
),

candles as (
    select
        symbol,
        trade_date as candle_time,
        min(day_low) as candle_low,
        max(day_high) as candle_high,
        any_value(candle_open) as candle_open,
        any_value(candle_close) as candle_close,
        avg(current_price) as trend_line
    from enriched
    group by symbol, trade_date
),

ranked as (
    select
        c.*,
        row_number() over (
            partition by symbol
            order by candle_time desc
        ) as rn
    from candles c
)

select
    symbol,
    candle_time,
    candle_low,
    candle_high,
    candle_open,
    candle_close,
    trend_line
from ranked
where rn <= 12
order by symbol, candle_time
