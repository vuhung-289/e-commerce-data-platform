-- Staging: extract USD/VND only — the key rate for conversion

with source as (
    select * from {{ source('raw', 'exchange_rate') }}
),

usd_rate as (
    select
        date(date)              as rate_date,
        currency_code,
        round(buy_rate, 0)      as buy_rate_vnd,
        round(sell_rate, 0)     as sell_rate_vnd,
        round(transfer_rate, 0) as transfer_rate_vnd,
        source                  as data_source,
        timestamp(ingested_at)  as ingested_at

    from source
    where
        currency_code = 'USD'
        and sell_rate is not null
        and sell_rate > 20000   -- sanity check: USD/VND can't be < 20k
)

select * from usd_rate