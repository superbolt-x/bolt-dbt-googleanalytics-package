{%- macro get_googleanalytics_clean_field(table_name, column_name) %}

    {%- if table_name == 'pages' -%}
        {%- if column_name == 'page_path' -%}
        {{column_name}},
        SPLIT_PART({{column_name}},'?',1) as page

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}

    {%- else -%}
    {{column_name}}

    {%- endif -%}

{% endmacro -%}