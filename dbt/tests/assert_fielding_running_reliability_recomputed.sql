-- Recompute every cell of mart_fielding_running_reliability from the raw
-- sources (not staging) with sums instead of corr(), and return any cell
-- that differs. Catches a pair joined across positions, a wrong bin edge,
-- a label taken from the player's main position instead of the row's,
-- a lost partial-season filter or a mis-parsed success rate. Partial
-- seasons are found from mart_pitcher_season here and from staging in the
-- model.
with partial_seasons as (
    select season from {{ ref("mart_pitcher_season") }} where is_partial group by season
),
o as (
    select player_id, season, position as pos,
           substr('1B2B3BSSLFCFRF', 2 * position - 5, 2) as lbl,
           outs_above_average as oaa, fielding_runs_prevented as frp,
           cast(replace(diff_success_rate_formatted, '%', '') as {{ dbt.type_int() }}) as srd
    from {{ source("raw", "oaa") }}
),
fp as (
    select x.lbl, x.oaa as a1, y.oaa as a2, x.frp as f1, y.frp as f2, x.srd as d1, y.srd as d2
    from o x join o y on y.player_id = x.player_id and y.pos = x.pos and y.season = x.season + 1
    left join partial_seasons p on p.season = y.season
    where p.season is null
),
sp as (
    select least(x.competitive_runs, y.competitive_runs) as mr,
           x.sprint_speed as s1, y.sprint_speed as s2, x.hp_to_1b as h1, y.hp_to_1b as h2
    from {{ source("raw", "sprint_speed") }} x
    join {{ source("raw", "sprint_speed") }} y on y.player_id = x.player_id and y.season = x.season + 1
    left join partial_seasons p on p.season = y.season
    where p.season is null
),
spb as (
    select *, case when mr >= 100 then '100+' when mr >= 50 then '50-99'
                   when mr >= 25 then '25-49' else '10-24' end as lbl,
              case when mr >= 100 then 100 when mr >= 50 then 50 when mr >= 25 then 25 else 10 end as lo,
              case when mr >= 100 then null when mr >= 50 then 99 when mr >= 25 then 49 else 24 end as hi
    from sp
),
raw_pairs as (
    select 'oaa' as metric, lbl, cast(null as {{ dbt.type_int() }}) as lo, cast(null as {{ dbt.type_int() }}) as hi, cast(a1 as {{ float_type() }}) as u, cast(a2 as {{ float_type() }}) as v from fp where a1 is not null and a2 is not null
    union all select 'fielding_runs_prevented', lbl, null, null, cast(f1 as {{ float_type() }}), cast(f2 as {{ float_type() }}) from fp where f1 is not null and f2 is not null
    union all select 'success_rate_diff', lbl, null, null, cast(d1 as {{ float_type() }}), cast(d2 as {{ float_type() }}) from fp where d1 is not null and d2 is not null
    union all select 'sprint_speed', lbl, lo, hi, s1, s2 from spb where s1 is not null and s2 is not null
    union all select 'hp_to_1b', lbl, lo, hi, h1, h2 from spb where h1 is not null and h2 is not null
),
sums as (
    select metric, lbl, lo, hi, count(*) as n,
           sum(u) as su, sum(v) as sv, sum(u * u) as suu, sum(v * v) as svv, sum(u * v) as suv
    from raw_pairs group by metric, lbl, lo, hi
),
expected as (
    select metric, lbl, lo, hi, n,
           (suv - su * sv / n) / sqrt((suu - su * su / n) * (svv - sv * sv / n)) as r,
           (sv - su) / n as dmean
    from sums
)
select e.*, m.min_runs_lo, m.min_runs_hi, m.n_pairs, m.yoy_corr, m.mean_change
from expected e
full join {{ ref("mart_fielding_running_reliability") }} m
  on m.metric = e.metric and m.group_label = e.lbl
where m.metric is null or e.metric is null
   or m.min_runs_lo is distinct from e.lo
   or m.min_runs_hi is distinct from e.hi
   or m.n_pairs <> e.n
   or abs(m.yoy_corr - e.r) > 1e-9
   or abs(m.mean_change - e.dmean) > 1e-9
