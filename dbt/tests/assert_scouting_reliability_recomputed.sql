-- Recompute every cell of mart_scouting_reliability another way (group-by
-- means and explicit joins instead of window functions and UNPIVOT) and
-- return any cell that differs. Catches a lost within-type centring, a
-- wrong bin edge or a pair joined across pitch types.
{% set metrics = ["usage", "whiff_rate", "run_value_per_100", "xwoba_allowed", "hard_hit_rate"] %}
with a as (
    select * from {{ ref("mart_pitch_arsenal_scouting") }} where pitches >= 100
),
{% for m in metrics %}
m_{{ m }} as (
    select season, pitch_type, avg({{ m }}) as mu from a group by season, pitch_type
),
p_{{ m }} as (
    select '{{ m }}' as metric,
           least(x.pitches, y.pitches) as mp,
           x.{{ m }} as v1, y.{{ m }} as v2,
           x.{{ m }} - mx.mu as w1, y.{{ m }} - my.mu as w2
    from a x
    join a y on y.player_id = x.player_id and y.pitch_type = x.pitch_type and y.season = x.season + 1
    join m_{{ m }} mx on mx.season = x.season and mx.pitch_type = x.pitch_type
    join m_{{ m }} my on my.season = y.season and my.pitch_type = y.pitch_type
    where x.{{ m }} is not null and y.{{ m }} is not null
),
{% endfor %}
allp as (
    {% for m in metrics %}select * from p_{{ m }}{% if not loop.last %} union all {% endif %}{% endfor %}
),
expected as (
    select metric,
           case when mp < 200 then 100 when mp < 400 then 200 when mp < 800 then 400 else 800 end as lo,
           count(*) as n, corr(v1, v2) as r_raw, corr(w1, w2) as r_within
    from allp group by 1, 2
)
select e.*, m.n_pairs, m.yoy_corr_raw, m.yoy_corr_within_type
from expected e
full join {{ ref("mart_scouting_reliability") }} m
  on m.metric = e.metric and m.min_pitches_lo = e.lo
where m.metric is null or e.metric is null
   or m.n_pairs <> e.n
   or abs(m.yoy_corr_raw - e.r_raw) > 1e-9
   or abs(m.yoy_corr_within_type - e.r_within) > 1e-9
