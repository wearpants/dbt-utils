{% macro get_tables_by_prefix(schema, prefix, exclude='', database=none) %}

    {%- call statement('tables', fetch_result=True) %}

      {{ dbt_utils.get_tables_by_prefix_sql(schema, prefix, exclude, database) }}

    {%- endcall -%}

    {%- set table_list = load_result('tables') -%}

    {%- if table_list and table_list['data'] -%}
        {%- set table_names = table_list['data'] | map(attribute=0) | list %}
        {%- set tables = [] -%}
        {%- for name in table_names -%}
            {%- set db_name, schema_name, table_name = (name | string).split(".") -%}
            {%- set table = api.Relation.create(database=db_name, schema=schema_name, identifier=table_name) -%}
            {% do tables.append(table) %}
        {%- endfor -%}
        {{ return(tables) }}
    {%- else -%}
        {{ return([]) }}
    {%- endif -%}

{% endmacro %}

