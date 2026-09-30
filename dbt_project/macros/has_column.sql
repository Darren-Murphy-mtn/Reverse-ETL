{# True when `column_name` exists on `relation`.

   GitHub omits optional keys entirely rather than sending nulls: `pull_request` appears only
   on pull requests, `label` only on labeled/unlabeled events. read_json(union_by_name) creates
   a column only if at least one record in the whole corpus carries the key, so an extraction
   with no PRs (e.g. --repos <sandbox>) or no label events produces Parquet without them and
   any model referencing them fails to bind. Guard those references with this macro.

   Returns false during parsing, when the relation can't be inspected; the guarded branch still
   compiles, and the real value is resolved at run time. #}
{% macro has_column(relation, column_name) -%}
    {%- if not execute -%}
        {{ return(false) }}
    {%- endif -%}
    {%- set columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
    {{ return(column_name | lower in columns) }}
{%- endmacro %}
