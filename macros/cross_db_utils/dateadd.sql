{% macro dateadd(datepart, interval, from_date_or_timestamp) %}
  {{ adapter_macro('dbt_utils.dateadd', datepart, interval, from_date_or_timestamp) }}
{% endmacro %}


{% macro default__dateadd(datepart, interval, from_date_or_timestamp) %}

    dateadd(
        {{ datepart }},
        {{ interval }},
        {{ from_date_or_timestamp }}
        )

{% endmacro %}


{% macro bigquery__dateadd(datepart, interval, from_date_or_timestamp) %}

        datetime_add(
            cast( {{ from_date_or_timestamp }} as datetime),
        interval {{ interval }} {{ datepart }}
        )

{% endmacro %}


{% macro postgres__dateadd(datepart, interval, from_date_or_timestamp) %}

    {{ from_date_or_timestamp }} + ((interval '1 {{ datepart }}') * ({{ interval }}))

{% endmacro %}


{% macro spark__dateadd(datepart, interval, from_date_or_timestamp) %}


    {% if datepart == 'day' %}

        date_add(date({{from_date_or_timestamp}}), {{interval}})

    {% elif datepart == 'month' %}

        add_months(date({{from_date_or_timestamp}}), {{interval}})

    {% else %}

        {{ exceptions.raise_compiler_error("macro datediff not implemented for this adapter") }}

    {% endif %}

{% endmacro %}
