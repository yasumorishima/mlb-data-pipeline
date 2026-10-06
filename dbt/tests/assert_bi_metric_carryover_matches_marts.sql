-- Every row of the four reliability marts appears in bi_metric_carryover
-- exactly once with the same numbers, and nothing else does. Returns rows
-- that are missing on either side or differ in any value.
with src as (
    select 'Batting' as area, metric, cast(min_pa_lo as {{ dbt.type_bigint() }}) as lo,
           cast(min_pa_hi as {{ dbt.type_bigint() }}) as hi,
           cast(null as {{ dbt.type_string() }}) as pos,
           cast(null as {{ dbt.type_string() }}) as lbl,
           n_pairs, yoy_corr as r, cast(null as {{ float_type() }}) as r_raw,
           next_woba_corr as r_next, cast(null as {{ float_type() }}) as chg
    from {{ ref("mart_batter_process_reliability") }}
    union all
    select 'Pitching', metric, cast(min_bf_lo as {{ dbt.type_bigint() }}),
           cast(min_bf_hi as {{ dbt.type_bigint() }}), null, null,
           n_pairs, yoy_corr, null, next_era_corr, null
    from {{ ref("mart_pitcher_process_reliability") }}
    union all
    select 'Pitch type', metric, cast(min_pitches_lo as {{ dbt.type_bigint() }}),
           cast(min_pitches_hi as {{ dbt.type_bigint() }}), null, null,
           n_pairs, yoy_corr_within_type, yoy_corr_raw, null, null
    from {{ ref("mart_scouting_reliability") }}
    union all
    select case when min_runs_lo is null then 'Fielding' else 'Running' end, metric,
           cast(min_runs_lo as {{ dbt.type_bigint() }}),
           cast(min_runs_hi as {{ dbt.type_bigint() }}),
           case when min_runs_lo is null then group_label end,
           group_label,
           n_pairs, yoy_corr, null, null, mean_change
    from {{ ref("mart_fielding_running_reliability") }}
),
bi as (
    select area, metric, group_lo as lo, group_hi as hi,
           case when sample_unit = 'position' then group_label end as pos,
           group_label, group_order, next_target,
           n_pairs, yoy_corr as r, yoy_corr_raw as r_raw, next_corr as r_next,
           mean_change as chg, next_corr_strength
    from {{ ref("bi_metric_carryover") }}
)
select coalesce(s.area, b.area) as area, coalesce(s.metric, b.metric) as metric,
       s.lo as src_lo, b.lo as bi_lo, s.pos as src_pos, b.pos as bi_pos,
       s.n_pairs as src_n, b.n_pairs as bi_n
from src s
full outer join bi b
    on b.area = s.area and b.metric = s.metric
   and coalesce(b.lo, -1) = coalesce(s.lo, -1)
   and coalesce(b.pos, '') = coalesce(s.pos, '')
where s.area is null or b.area is null
   or s.n_pairs != b.n_pairs
   or coalesce(s.hi, -1) != coalesce(b.hi, -1)
   -- The fielding/running mart writes its own labels ('10-24', '100+', 'SS'),
   -- which also checks the model's formatting of the other marts' bins.
   or (s.lbl is not null and s.lbl != b.group_label)
   or (s.lbl is null and b.group_label != case when s.hi is null then concat(cast(s.lo as {{ dbt.type_string() }}), '+')
        else concat(cast(s.lo as {{ dbt.type_string() }}), '-', cast(s.hi as {{ dbt.type_string() }})) end)
   or coalesce(b.next_target, '') != case s.area when 'Batting' then 'wOBA' when 'Pitching' then 'ERA' else '' end
   -- Bins sort by their lower edge, positions in scorebook order.
   or b.group_order != coalesce(s.lo, case s.pos when '1B' then 3 when '2B' then 4 when '3B' then 5
        when 'SS' then 6 when 'LF' then 7 when 'CF' then 8 when 'RF' then 9 end)
   or coalesce(s.r, 9) != coalesce(b.r, 9)
   or coalesce(s.r_raw, 9) != coalesce(b.r_raw, 9)
   or coalesce(s.r_next, 9) != coalesce(b.r_next, 9)
   or coalesce(s.chg, 9) != coalesce(b.chg, 9)
   or coalesce(abs(s.r_next), 9) != coalesce(b.next_corr_strength, 9)
