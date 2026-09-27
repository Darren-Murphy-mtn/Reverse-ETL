{# Per-repo staleness threshold from the stale_days_by_repo var, falling back to stale_days_default. #}
{% macro stale_threshold_days(repo_column) -%}
    {%- set overrides = var('stale_days_by_repo', {}) -%}
    {%- if overrides -%}
    case {{ repo_column }}
        {%- for repo, days in overrides.items() %}
        when '{{ repo }}' then {{ days }}
        {%- endfor %}
        else {{ var('stale_days_default') }}
    end
    {%- else -%}
    {{ var('stale_days_default') }}
    {%- endif -%}
{%- endmacro %}
