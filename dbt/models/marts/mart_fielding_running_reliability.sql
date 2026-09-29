-- How much the fielding and running numbers carry over from one season to
-- the next.
--
-- Fielding (oaa, fielding_runs_prevented, success_rate_diff): a pair is the
-- same player at the same position in consecutive seasons, grouped by that
-- position. Savant's OAA table has no attempt count, so there is no
-- sample-size bin; the source only lists qualified fielders, which also
-- narrows the spread. oaa and fielding_runs_prevented are totals, so part of
-- what carries over is playing time; success_rate_diff (actual minus
-- expected catch rate, whole points) is the rate.
--
-- Running (sprint_speed, hp_to_1b): a pair is the same player in
-- consecutive seasons, binned by the smaller of the two competitive-run
-- counts (the source starts at 10). hp_to_1b is only there for players with
-- enough timed runs to first.
--
-- mean_change is the average of next season minus this season (aging shows
-- up here for sprint speed). Pairs whose second season is still in progress
-- are left out, as in mart_scouting_reliability.
with partial_seasons as (
    select distinct season from {{ ref("stg_statsapi__pitching") }} where is_partial
),
o as (
    select * from {{ ref("stg_savant__oaa") }}
),
fielding_pairs as (
    {% for m in ["oaa", "fielding_runs_prevented", "success_rate_diff"] %}
    select
        '{{ m }}' as metric,
        x.primary_position as group_label,
        cast(null as {{ dbt.type_int() }}) as min_runs_lo,
        cast(x.{{ m }} as {{ float_type() }}) as v1,
        cast(y.{{ m }} as {{ float_type() }}) as v2
    from o x
    join o y
      on y.player_id = x.player_id and y.position_code = x.position_code
     and y.season = x.season + 1
    where y.season not in (select season from partial_seasons)
      and x.{{ m }} is not null and y.{{ m }} is not null
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),
s as (
    select * from {{ ref("stg_savant__sprint_speed") }}
),
running_pairs as (
    {% for m in ["sprint_speed", "hp_to_1b"] %}
    select
        '{{ m }}' as metric,
        least(x.competitive_runs, y.competitive_runs) as min_runs,
        x.{{ m }} as v1,
        y.{{ m }} as v2
    from s x
    join s y on y.player_id = x.player_id and y.season = x.season + 1
    where y.season not in (select season from partial_seasons)
      and x.{{ m }} is not null and y.{{ m }} is not null
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),
running_binned as (
    select
        metric,
        case when min_runs < 25 then '10-24' when min_runs < 50 then '25-49'
             when min_runs < 100 then '50-99' else '100+' end as group_label,
        case when min_runs < 25 then 10 when min_runs < 50 then 25
             when min_runs < 100 then 50 else 100 end as min_runs_lo,
        v1, v2
    from running_pairs
),
allp as (
    select * from fielding_pairs
    union all
    select * from running_binned
)
select
    metric,
    group_label,
    min_runs_lo,
    case min_runs_lo when 10 then 24 when 25 then 49 when 50 then 99 end as min_runs_hi,
    count(*)            as n_pairs,
    corr(v1, v2)        as yoy_corr,
    avg(v2 - v1)        as mean_change
from allp
group by metric, group_label, min_runs_lo
