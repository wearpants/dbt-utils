{% macro nullcheck_table(schema, table, database=none) %}
  {%- if database is none -%}
  	{%- set database = context.database -%}
  {%- endif -%}
  {% set relation = api.Relation.create(database=database, schema=schema, identifier=table) %}
  {% set cols = adapter.get_columns_in_relation(relation) %}

  select {{ dbt_utils.nullcheck(cols) }}
  from {{ relation }}

{% endmacro %}
