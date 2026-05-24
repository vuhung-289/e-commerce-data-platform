-- Mart: daily summary table, one row per day
-- Joins all 3 sources, computes dashboard metrics
-- Incremental: only processes new data, no full recompute

{{
    config(
        materialized='incremental',
        unique_key='order_date',
        on_schema_change='sync_all_columns'
    )
}}

with transactions as (
    select * from {{ ref('stg_transactions') }}
    {% if is_incremental() %}
        -- Only fetch last 3 days to handle late-arriving data from execution date
        where order_date >= date_sub(cast('{{ var("execution_date", run_started_at.strftime("%Y-%m-%d")) }}' as date), interval 3 day)
    {% endif %}
),

exchange_rate as (
    select * from {{ ref('stg_exchange_rate') }}
),

news as (
    select * from {{ ref('stg_news') }}
),

-- FIX: Pre-aggregate top_category and top_payment to avoid correlated subquery
-- Top category by GMV per day
top_category as (
    select
        order_date,
        category as top_category_by_gmv
    from (
        select
            order_date,
            category,
            sum(total_amount_vnd) as cat_gmv,
            row_number() over (
                partition by order_date
                order by sum(total_amount_vnd) desc
            ) as rn
        from transactions
        where is_revenue_generating
        group by order_date, category
    )
    where rn = 1
),

-- Top payment method per day
top_payment as (
    select
        order_date,
        payment_method as top_payment_method
    from (
        select
            order_date,
            payment_method,
            count(*) as cnt,
            row_number() over (
                partition by order_date
                order by count(*) desc
            ) as rn
        from transactions
        group by order_date, payment_method
    )
    where rn = 1
),

-- Aggregate transactions by day
daily_txn as (
    select
        t.order_date,

        -- Volume metrics
        count(*)                                                as total_orders,
        countif(is_revenue_generating)                         as revenue_orders,
        countif(order_status = 'cancelled')                    as cancelled_orders,
        countif(order_status = 'returned')                     as returned_orders,
        count(distinct customer_id)                            as unique_customers,

        -- Revenue metrics (VND)
        sum(case when is_revenue_generating
            then total_amount_vnd else 0 end)                  as gmv_vnd,
        sum(case when is_revenue_generating
            then discount_amount_vnd else 0 end)               as total_discount_vnd,
        sum(case when is_revenue_generating
            then shipping_fee_vnd else 0 end)                  as total_shipping_vnd,
        avg(case when is_revenue_generating
            then total_amount_vnd end)                         as avg_order_value_vnd,

        -- Rates
        round(
            countif(order_status = 'cancelled') * 100.0 / count(*), 2
        )                                                      as cancellation_rate_pct,
        round(
            countif(order_status = 'returned') * 100.0 / count(*), 2
        )                                                      as return_rate_pct,

        -- Platform breakdown
        countif(platform = 'shopee')                           as orders_shopee,
        countif(platform = 'tiki')                             as orders_tiki,
        countif(platform = 'lazada')                           as orders_lazada,
        countif(platform = 'sendo')                            as orders_sendo

    from transactions t
    group by t.order_date
),

-- FIX: JOIN instead of correlated subquery
daily_with_tops as (
    select
        d.*,
        tc.top_category_by_gmv,
        tp.top_payment_method
    from daily_txn d
    left join top_category tc on d.order_date = tc.order_date
    left join top_payment  tp on d.order_date = tp.order_date
),

-- Join with exchange rate
with_fx as (
    select
        d.*,
        e.sell_rate_vnd                                              as usd_sell_rate,
        round(d.gmv_vnd / nullif(e.sell_rate_vnd, 0), 2)            as gmv_usd,
        round(d.avg_order_value_vnd / nullif(e.sell_rate_vnd, 0), 2) as avg_order_value_usd,
        e.data_source                                                as fx_data_source

    from daily_with_tops d
    left join exchange_rate e
        on d.order_date = e.rate_date
),

-- Join with news events
final as (
    select
        f.*,
        coalesce(n.total_articles, 0)                          as news_total_articles,
        coalesce(n.ecommerce_articles, 0)                      as news_ecommerce_articles,
        coalesce(n.has_major_ecommerce_event, false)           as has_major_ecommerce_event,
        current_timestamp()                                    as dbt_updated_at

    from with_fx f
    left join news n
        on f.order_date = n.news_date
)

select * from final