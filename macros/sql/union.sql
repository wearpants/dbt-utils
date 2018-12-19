{% macro union_tables(tables, column_override=none, exclude=none) -%}

    {#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}

    {%- set exclude = exclude if exclude is not none else [] %}
    {%- set column_override = column_override if column_override is not none else {} %}

    {%- set table_columns = {} %}
    {%- set column_superset = {} %}

    {%- for table in tables -%}

        {%- do table_columns.update({table: []}) %}

        {%- set cols = adapter.get_columns_in_relation(table) %}
        {%- for col in cols -%}

        {%- if col.column not in exclude %}

            {# update the list of columns in this table #}
            {%- do table_columns[table].append(col.column) %}

            {%- if col.column in column_superset -%}

                {%- set stored = column_superset[col.column] %}
                {%- if col.is_string() and stored.is_string() and col.string_size() > stored.string_size() -%}

                    {%- do column_superset.update({col.column: col}) %}

                {%- endif %}

            {%- else -%}

                {%- do column_superset.update({col.column: col}) %}

            {%- endif -%}

        {%- endif -%}

        {%- endfor %}
    {%- endfor %}

    {%- set ordered_column_names = column_superset.keys() %}

    {%- for table in tables -%}

        (
            select

                {{ dbt_utils.safe_cast(dbt_utils.string_literal(table), dbt_utils.type_string()) }} as _dbt_source_table,

                {% for col_name in ordered_column_names -%}

                    {%- set col = column_superset[col_name] %}
                    {%- set col_type = column_override.get(col.column, col.data_type) %}
                    {%- set col_name = adapter.quote(col_name) if col_name in table_columns[table] else 'null' %}

                    {{ dbt_utils.safe_cast(col_name, col_type) }} as {{ col.quoted }} {% if not loop.last %},{% endif %}
                {%- endfor %}

            from {{ table }}
        )

        {% if not loop.last %} union all {% endif %}

    {%- endfor %}

{%- endmacro %}
