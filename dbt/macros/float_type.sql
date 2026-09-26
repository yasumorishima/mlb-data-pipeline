{#- A 64-bit float on every adapter. dbt.type_float() is FLOAT on DuckDB,
    which is 32-bit there, so it is not used. -#}
{% macro float_type() %}{{ return(adapter.dispatch("float_type")()) }}{% endmacro %}
{% macro default__float_type() %}double{% endmacro %}
{% macro bigquery__float_type() %}float64{% endmacro %}
