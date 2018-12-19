{#
Pivot values from columns to rows.

Example Usage: {{ dbt_utils.unpivot(table=ref('users'), cast_to='integer', exclude=['id','created_at']) }}

Arguments:
    table: Table name, required.
    cast_to: The datatype to cast all unpivoted columns to. Default is varchar.
    exclude: A list of columns to exclude from the unpivot operation. Default is none.
#}

{% macro unpivot(table, cast_to='varchar', exclude=none) -%}

  {%- set exclude = (exclude | map('upper')) if exclude is not none else [] %}

  {%- set include_cols = [] %}

  {%- set table_columns = {} %}

  {%- do table_columns.update({table: []}) -%}

  {%- set cols = adapter.get_columns_in_relation(table) %}

  {%- for col in cols -%}
    {%- if (col.column | upper) not in exclude -%}
      {% do include_cols.append(col) %}
    {%- endif %}
  {%- endfor %}

  {%- for col in include_cols -%}

    select
      {%- for exclude_col in exclude %}
        {{ exclude_col }},
      {%- endfor %}
      cast('{{ col.column }}' as {{ dbt_utils.type_string() }}) as field_name,
      {{ dbt_utils.safe_cast(field=col.column, type=cast_to) }} as value
    from {{ table }}
    {% if not loop.last -%}
      union all
    {% endif -%}
  {%- endfor -%}
{%- endmacro %}
