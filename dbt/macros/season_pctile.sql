{#- Percentile 0-100 of `col` within a season, among the rows where
    `qualified` is true and `col` is not NULL; NULL for every other row and
    when fewer than two rows qualify. 100 is always the best end: pass
    lower_is_better=true for metrics such as K% for a batter or FIP.
    The population flag is part of the partition, not a filter, so NULLs and
    unqualified rows never take a rank (DuckDB and BigQuery sort NULLs to
    opposite ends). -#}
{% macro season_pctile(col, qualified, lower_is_better=false) -%}
case when ({{ qualified }}) and {{ col }} is not null
      and count(*) over (partition by season, (({{ qualified }}) and {{ col }} is not null)) > 1 then
    cast(round(100 * percent_rank() over (
        partition by season, (({{ qualified }}) and {{ col }} is not null)
        order by {{ col }}{{ " desc" if lower_is_better }}
    )) as {{ dbt.type_bigint() }})
end
{%- endmacro %}
