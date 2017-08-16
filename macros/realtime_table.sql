

{% macro get_table_sql(sql, unique_key, updated_at) %}

    with data as (

        {{ sql }}

    )

    select
        {{ unique_key }} as __dbt_pk,
        {{ updated_at }} as __dbt_updated_at,
        *
    from data

{% endmacro %}

{% macro get_view_sql(table_relation, sql, unique_key, updated_at) %}

    with data as (

        {{ sql }}

    ), renamed as (

        select
            {{ unique_key }} as __dbt_pk,
            {{ updated_at }} as __dbt_updated_at,
            *
        from data

    ), materialized as (

        select
            max(__dbt_updated_at) as progress
        from {{ table_relation }}

    )

    select *
    from renamed
    where __dbt_updated_at > (select progress from materialized)

{% endmacro %}

{% macro get_union_sql(table_relation, view_relation) %}

    with joined as (

        select 'old' as __dbt_source, * from {{ table_relation }}
        union all
        select 'new' as __dbt_source, * from {{ view_relation }}

    ), ranked as (

        select *,
            row_number() over (partition by __dbt_pk order by __dbt_source = 'new' desc)
        from joined

    )

    -- TODO : get_columns_in_table to exclude junk!
    select *
    from ranked
    where row_number = 1

{% endmacro %}


{% materialization nrt, default %}

    {%- set identifier = model['name'] %}
    {%- set unique_key = config.get('unique_key') -%}
    {%- set updated_at = config.get('updated_at') -%}

    {% set table_identifier = identifier ~ '__table' %}
    {% set view_identifier = identifier ~ '__view' %}

    {% set table_relation = adapter.quote_schema_and_table(schema, table_identifier) %}
    {% set view_relation = adapter.quote_schema_and_table(schema, view_identifier) %}

    {% set table_create_sql = get_table_sql(sql, unique_key, updated_at) %}
    {% set view_create_sql = get_view_sql(table_relation, sql, unique_key, updated_at) %}

    {% call statement('main') %}

        {{ create_table_as(False, table_identifier, table_create_sql) }}
        {{ create_view_as(view_identifier, view_create_sql) }}

        {% set union_sql = get_union_sql(table_relation, view_relation) %}
        {{ create_view_as(identifier, union_sql) }}

    {% endcall %}

    {{ adapter.commit() }}

{% endmaterialization %}
