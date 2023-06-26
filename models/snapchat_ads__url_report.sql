{{ config(enabled=var('ad_reporting__snapchat_ads_enabled', true)) }}

with ad_hourly as (

    select *
    from {{ var('ad_hourly_report') }}

), ad_daily as (

    select
        ad_id,
        cast(date_hour as date) as date_day,
        sum(swipes) as clicks, --renamed field
        sum(impressions) as impressions,
        round(sum(spend),2) as spend
    from ad_hourly
    {{ dbt_utils.group_by(2) }}

), creatives as (

    select *
    from {{ ref('snapchat_ads__creative_history_prep') }}

), account as (

    select *
    from {{ var('ad_account_history') }}
    where is_most_recent_record = true

), ads as (

    select *
    from {{ var('ad_history') }}
    where is_most_recent_record = true

), ad_squads as (

    select *
    from {{ var('ad_squad_history') }}
    where is_most_recent_record = true

), campaigns as (

    select *
    from {{ var('campaign_history') }}
    where is_most_recent_record = true


), joined as (

    select
        ad_daily.date_day,
        account.ad_account_id,
        account.ad_account_name,
        ad_daily.ad_id,
        ads.ad_name,
        ad_squads.ad_squad_id,
        ad_squads.ad_squad_name,
        campaigns.campaign_id,
        campaigns.campaign_name,
        account.currency,
        creatives.creative_name,
        creatives.base_url,
        creatives.url_host,
        creatives.url_path,
        creatives.utm_source,
        creatives.utm_medium,
        creatives.utm_campaign,
        creatives.utm_content,
        creatives.utm_term,
        ad_daily.clicks,
        ad_daily.impressions,
        ad_daily.spend
    
    from ad_daily
    left join ads 
        on ad_daily.ad_id = ads.ad_id
    left join ad_squads
        on ads.ad_squad_id = ad_squads.ad_squad_id
    left join campaigns
        on ad_squads.campaign_id = campaigns.campaign_id
    left join account
        on campaigns.ad_account_id = account.ad_account_id
    left join creatives
        on ads.creative_id = creatives.creative_id

)

select *
from joined
