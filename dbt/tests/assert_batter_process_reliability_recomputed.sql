-- Recompute every cell of mart_batter_process_reliability another way
-- (one explicit join per metric instead of UNPIVOT) and return any cell that
-- differs. Catches a wrong bin edge, a pair joined across metrics or next
-- season's wOBA taken from the wrong row.
{% set metrics = ["woba", "xwoba", "woba_minus_xwoba", "babip", "k_rate", "bb_rate", "iso"] %}
with b as (
    select * from {{ ref("mart_batter_season") }} where pa >= 100
),
{% for m in metrics %}
p_{{ m }} as (
    select '{{ m }}' as metric,
           least(x.pa, y.pa) as mp,
           x.{{ m }} as v1, y.{{ m }} as v2, y.woba as nw
    from b x
    join b y on y.player_id = x.player_id and y.season = x.season + 1
    where x.{{ m }} is not null and y.{{ m }} is not null and not y.is_partial
),
{% endfor %}
allp as (
    {% for m in metrics %}select * from p_{{ m }}{% if not loop.last %} union all {% endif %}{% endfor %}
),
expected as (
    select metric,
           case when mp < 200 then 100 when mp < 400 then 200 when mp < 600 then 400 else 600 end as lo,
           case when mp < 200 then 199 when mp < 400 then 399 when mp < 600 then 599 end as hi,
           count(*) as n, corr(v1, v2) as r, corr(v1, nw) as r_next
    from allp group by 1, 2, 3
)
select e.*, m.n_pairs, m.yoy_corr, m.next_woba_corr
from expected e
full join {{ ref("mart_batter_process_reliability") }} m
  on m.metric = e.metric and m.min_pa_lo = e.lo
where m.metric is null or e.metric is null
   or m.n_pairs <> e.n
   or m.min_pa_hi is distinct from e.hi
   or abs(m.yoy_corr - e.r) > 1e-9
   or abs(m.next_woba_corr - e.r_next) > 1e-9
