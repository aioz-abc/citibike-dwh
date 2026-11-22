{# Модели пишутся ровно в схему из профиля (ozbekova_datamart), без префиксов dbt. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}
