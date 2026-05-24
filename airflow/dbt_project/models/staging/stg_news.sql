-- Staging: filter e-commerce relevant news, create daily event flag

with source as (
    select * from {{ source('raw', 'news') }}
),

relevant_news as (
    select
        date(date)                      as news_date,
        source,
        title,
        summary,
        link,
        timestamp(published_at)         as published_at,
        is_ecommerce_relevant,
        timestamp(ingested_at)          as ingested_at

    from source
    where title is not null
),

-- Aggregate to daily: article count per day, event flag
daily_news as (
    select
        news_date,
        count(*)                                        as total_articles,
        countif(is_ecommerce_relevant = true)           as ecommerce_articles,
        case
            when countif(is_ecommerce_relevant = true) >= 3
            then true else false
        end                                             as has_major_ecommerce_event

    from relevant_news
    group by news_date
)

select * from daily_news