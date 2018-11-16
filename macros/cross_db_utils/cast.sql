{% macro cast(field, type) %}
  {{ adapter_macro('dbt_utils.cast', field, type) }}
{% endmacro %}


{% macro default__cast(field, type) %}
    {{field}}::{{type}}
{% endmacro %}

{% macro bigquery__cast(field, type) %}
    cast({{field}} as {{type}})
{% endmacro %}