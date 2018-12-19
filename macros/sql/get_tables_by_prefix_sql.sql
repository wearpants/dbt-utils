{% macro get_tables_by_prefix_sql(schema, prefix, exclude='', database=none) %}
    {%- if database is none -%}
        {%- set database = context.database -%}
    {%- endif -%}

    {{ adapter_macro('dbt_utils.get_tables_by_prefix_sql', schema, prefix, exclude, database) }}
{% endmacro %}

{% macro default__get_tables_by_prefix_sql(schema, prefix, exclude, database) %}

        select distinct
            table_catalog || '.' || table_schema || '.' || table_name as ref
        from information_schema.tables
        where table_schema = '{{ schema }}'
        and table_catalog = '{{ database }}'
        and table_name ilike '{{ prefix }}%'
        and table_name not ilike '{{ exclude }}'

{% endmacro %}


{% macro bigquery__get_tables_by_prefix_sql(schema, prefix, exclude, database) %}

        select distinct
            concat(project_id, '.', dataset_id, '.', table_id) as ref

        from {{ adapter.quote(database) }}.{{schema}}.__TABLES_SUMMARY__
        where dataset_id = '{{schema}}'
            and project_id = '{{database}}'
            and lower(table_id) like lower ('{{prefix}}%')
            and lower(table_id) not like lower ('{{exclude}}')

{% endmacro %}
