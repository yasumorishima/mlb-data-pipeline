-- Recompute every cell of mart_pitcher_process_reliability another way
-- (one explicit join per metric instead of UNPIVOT) and return any cell that
-- differs. Catches a wrong bin edge, a pair joined across metrics or next
-- season's ERA taken from the wrong row.
{% set metrics = ["era", "fip", "xera", "era_minus_xera", "k_minus_bb_rate", "k_rate", "bb_rate", "xwoba_allowed"] %}
with p as (
    select * from {{ ref("mart_pitcher_season") }} where bf >= 100 and xera is not null
),
{% for m in metrics %}
p_{{ m }} as (
    select '{{ m }}' as metric,
           least(x.bf, y.bf) as mb,
           x.{{ m }} as v1, y.{{ m }} as v2, y.era as ne
    from p x
    join p y on y.player_id = x.player_id and y.season = x.season + 1
    where x.{{ m }} is not null and y.{{ m }} is not null and not y.is_partial
),
{% endfor %}
allp as (
    {% for m in metrics %}select * from p_{{ m }}{% if not loop.last %} union all {% endif %}{% endfor %}
),
expected as (
    select metric,
           case when mb < 200 then 100 when mb < 400 then 200 when mb < 600 then 400 else 600 end as lo,
           case when mb < 200 then 199 when mb < 400 then 399 when mb < 600 then 599 end as hi,
           count(*) as n, corr(v1, v2) as r, corr(v1, ne) as r_next
    from allp group by 1, 2, 3
)
select e.*, m.n_pairs, m.yoy_corr, m.next_era_corr
from expected e
full join {{ ref("mart_pitcher_process_reliability") }} m
  on m.metric = e.metric and m.min_bf_lo = e.lo
where m.metric is null or e.metric is null
   or m.n_pairs <> e.n
   or m.min_bf_hi is distinct from e.hi
   or abs(m.yoy_corr - e.r) > 1e-9
   or abs(m.next_era_corr - e.r_next) > 1e-9
