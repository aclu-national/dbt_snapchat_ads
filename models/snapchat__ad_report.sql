with ad_hourly as (

    select *
    from {{ var('ad_hourly_report') }}

), creatives as (

    select *
    from {{ ref('snapchat__creative_history_prep') }}

), account as (

    select *
    from {{ var('ad_account_history') }}
    where is_most_recent_record = true

), ads as (

    select *
    from {{ var('ad_history') }}
    where is_most_recent_record = true

), aggregated as (

    select
        'snapchat_ads' as platform, 
        cast(ad_hourly.date_hour as date) as date_day,
        account.ad_account_id,
        account.ad_account_name,
        ads.ad_id,
        ads.ad_name,
        account.currency,
        sum(ad_hourly.swipes) as swipes,
        sum(ad_hourly.impressions) as impressions,
        round(sum(ad_hourly.spend)) as spend
    
    from ad_hourly
    left join ads 
        on ad_hourly.ad_id = ads.ad_id
    left join creatives
        on ads.creative_id = creatives.creative_id
    left join account
        on creatives.ad_account_id = account.ad_account_id
    
    {{ dbt_utils.group_by(7) }}

)

select *
from aggregated