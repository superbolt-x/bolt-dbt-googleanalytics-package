{{ config (
    alias = target.database + '_googleanalytics_performance_by_campaign'
)}}
{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH 
    {% for date_granularity in date_granularity_list -%}

    performance_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        profile,
        source_medium,
        campaign,
        COALESCE(SUM(sessions),0) as sessions,
        COALESCE(SUM(new_sessions),0) as new_sessions,
        COALESCE(SUM(bounced_sessions),0) as bounced_sessions,
        COALESCE(SUM(session_duration),0) as session_duration,
        COALESCE(SUM(pageviews),0) as pageviews,
        COALESCE(SUM(transactions),0) as purchases,
        COALESCE(SUM(transaction_revenue),0) as revenue
        
    FROM {{ ref('googleanalytics_traffic_sources') }}
    GROUP BY 1,2,3,4,5)

    {%- if not loop.last %},

    {% endif %}
    {%- endfor %}

{% for date_granularity in date_granularity_list -%}
SELECT * 
FROM performance_{{date_granularity}}
{% if not loop.last %}UNION ALL
{% endif %}
{%- endfor %}