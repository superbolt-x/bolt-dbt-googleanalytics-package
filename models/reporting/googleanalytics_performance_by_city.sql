{{ config (
    alias = target.database + '_googleanalytics_performance_by_city'
)}}
{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH 
    {% for date_granularity in date_granularity_list -%}

    performance_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        profile,
        country,
        region,
        city,
        COALESCE(SUM(sessions),0) as sessions,
        COALESCE(SUM(new_users),0) as new_users,
        COALESCE(SUM(bounced_sessions),0) as bounced_sessions,
        COALESCE(SUM(session_duration),0) as session_duration,
        COALESCE(SUM(pageviews),0) as pageviews
        
    FROM {{ ref('googleanalytics_locations') }}
    GROUP BY 1,2,3,4,5,6)

    {%- if not loop.last %},

    {% endif %}
    {%- endfor %}

{% for date_granularity in date_granularity_list -%}
SELECT * 
FROM performance_{{date_granularity}}
{% if not loop.last %}UNION ALL
{% endif %}
{%- endfor %}