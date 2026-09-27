{% macro resolution_bucket(hours_column) -%}
    case
        when {{ hours_column }} < 24 then 'same_day'
        when {{ hours_column }} < 24 * 7 then 'under_1_week'
        when {{ hours_column }} < 24 * 30 then 'under_1_month'
        else 'over_1_month'
    end
{%- endmacro %}
