-- The pitcher counterpart of mart_batter_process_reliability: how much each
-- run-prevention number carries over from one season to the next, by sample
-- size, and how well it predicts next season's ERA. A pair is the same
-- pitcher in consecutive seasons with at least 100 batters faced in each;
-- its size bin is the smaller of the two BF counts.
--
-- yoy_corr asks whether the number repeats: the same metric in both
-- seasons. next_era_corr asks whether it is the better guide: this season's
-- metric against next season's ERA (a positive sign for ERA, FIP, xERA and
-- xwOBA allowed, a negative one for strikeout rates). The questions: does a
-- pitcher who beat his xERA keep beating it (era_minus_xera), and is FIP or
-- xERA a better read of next season's ERA than ERA itself.
--
-- It mixes sampling noise, real change and range restriction within the
-- bin, so it is how well one season predicts the next, not a within-season
-- reliability. Pairs whose second season is still in progress are left out,
-- and so are pitcher-seasons with no Statcast xERA, so every metric in a bin
-- is measured on the same pairs.
with p as (
    select * from {{ ref("mart_pitcher_season") }}
    where bf >= 100 and xera is not null
),
long as (
    select player_id, season, bf, is_partial, metric, value
    from p
    unpivot (value for metric in (era, fip, xera, era_minus_xera, k_minus_bb_rate, k_rate, bb_rate, xwoba_allowed))
),
pairs as (
    select
        x.metric,
        least(x.bf, y.bf) as min_bf,
        x.value as v1, y.value as v2,
        np.era as next_era
    from long x
    join long y
      on y.player_id = x.player_id and y.metric = x.metric
     and y.season = x.season + 1
    -- next season's ERA comes from p, not from the unpivoted rows: BigQuery
    -- drops a column that only renames an unpivoted one
    join p np on np.player_id = y.player_id and np.season = y.season
    where not y.is_partial
),
binned as (
    select *,
        case when min_bf < 200 then 100
             when min_bf < 400 then 200
             when min_bf < 600 then 400
             else 600 end as min_bf_lo
    from pairs
)
select
    metric,
    min_bf_lo,
    case min_bf_lo when 100 then 199 when 200 then 399 when 400 then 599 end as min_bf_hi,
    count(*)             as n_pairs,
    corr(v1, v2)         as yoy_corr,
    corr(v1, next_era)   as next_era_corr
from binned
group by metric, min_bf_lo
