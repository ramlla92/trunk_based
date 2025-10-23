{% macro convert_percent_to_decimal(percent_string) %}
  CAST(REPLACE({{ percent_string }}, '%', '') AS FLOAT) / 100
{% endmacro %}
