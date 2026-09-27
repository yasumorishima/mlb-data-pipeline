-- How much each batting number carries over from one season to the next,
-- by sample size, and how well it predicts next season's wOBA. A pair is
-- the same batter in consecutive seasons with at least 100 PA in each; its
-- size bin is the smaller of the two PA counts.
--
-- yoy_corr asks whether the number is a skill: the same metric in both
-- seasons. next_woba_corr asks whether it is the better guide: this
-- season's metric against next season's wOBA. The table was built for two
-- questions: does a batter who beat his xwOBA keep beating it
-- (woba_minus_xwoba), and is xwOBA a better read of next season than wOBA.
--
-- Like mart_scouting_reliability it mixes sampling noise, real change and
-- range restriction within the bin, so it is how well one season predicts
-- the next, not a within-season reliability. Pairs whose second season is
-- still in progress are left out.
with b as (
    select * from {{ ref("mart_batter_season") }}
    where pa >= 100
),
long as (
    select player_id, season, pa, is_partial, metric, value
    from b
    unpivot (value for metric in (woba, xwoba, woba_minus_xwoba, babip, k_rate, bb_rate, iso))
),
pairs as (
    select
        x.metric,
        least(x.pa, y.pa) as min_pa,
        x.value as v1, y.value as v2,
        nb.woba as next_woba
    from long x
    join long y
      on y.player_id = x.player_id and y.metric = x.metric
     and y.season = x.season + 1
    -- next season's wOBA comes from b, not from the unpivoted rows: BigQuery
    -- drops a column that only renames an unpivoted one (woba as next_woba)
    join b nb on nb.player_id = y.player_id and nb.season = y.season
    where not y.is_partial
),
binned as (
    select *,
        case when min_pa < 200 then 100
             when min_pa < 400 then 200
             when min_pa < 600 then 400
             else 600 end as min_pa_lo
    from pairs
)
select
    metric,
    min_pa_lo,
    case min_pa_lo when 100 then 199 when 200 then 399 when 400 then 599 end as min_pa_hi,
    count(*)              as n_pairs,
    corr(v1, v2)          as yoy_corr,
    corr(v1, next_woba)   as next_woba_corr
from binned
group by metric, min_pa_lo
