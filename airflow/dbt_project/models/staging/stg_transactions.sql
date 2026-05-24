-- Staging: clean and standardize transactions
-- Filter bad data, rename columns, cast types

with source as (
    select * from {{ source('raw', 'transactions') }}
),

cleaned as (
    select
        transaction_id,
        timestamp(order_timestamp)          as order_timestamp,
        date(date)                          as order_date,
        lower(platform)                     as platform,
        category,
        customer_id,
        customer_city,

        -- Only completed and returned orders count as revenue
        order_status,
        case
            when order_status in ('completed', 'returned') then true
            else false
        end                                 as is_revenue_generating,

        cast(quantity as int64)             as quantity,
        round(unit_price_vnd, 0)            as unit_price_vnd,
        round(total_amount_vnd, 0)          as total_amount_vnd,
        round(discount_amount_vnd, 0)       as discount_amount_vnd,
        round(shipping_fee_vnd, 0)          as shipping_fee_vnd,
        cast(discount_pct as int64)         as discount_pct,
        payment_method,
        timestamp(ingested_at)              as ingested_at

    from source
    where
        -- Filter corrupt records
        transaction_id is not null
        and total_amount_vnd > 0
        and total_amount_vnd < 500000000    -- cap 500M VND, filter outliers
)

select * from cleaned