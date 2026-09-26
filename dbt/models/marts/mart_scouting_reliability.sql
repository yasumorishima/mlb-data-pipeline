-- How much each pitch metric carries over from one season to the next, by
-- sample size. A pair is the same pitcher and pitch type in consecutive
-- seasons; its size bin is the smaller of the two pitch counts.
--
-- yoy_corr_within_type first subtracts the mean of that pitch type in that
-- season (among pitches >= 100), so it measures what scouting compares:
-- a slider against other sliders. yoy_corr_raw keeps the gap between pitch
-- types (sliders miss more bats than sinkers), which inflates it.
--
-- The year-to-year correlation mixes sampling noise with real change in
-- the pitch, so it is a floor on the metric's reliability within a season.
with a as (
    select * from {{ ref("mart_pitch_arsenal_scouting") }}
    where pitches >= 100
),
long as (
    select player_id, season, pitch_type, pitches, metric, value,
           value - avg(value) over (partition by season, pitch_type, metric) as value_within_type
    from a
    unpivot (value for metric in (usage, whiff_rate, run_value_per_100, xwoba_allowed, hard_hit_rate))
),
pairs as (
    select
        x.metric,
        least(x.pitches, y.pitches) as min_pitches,
        x.value as v1, y.value as v2,
        x.value_within_type as w1, y.value_within_type as w2
    from long x
    join long y
      on y.player_id = x.player_id and y.pitch_type = x.pitch_type
     and y.metric = x.metric and y.season = x.season + 1
),
binned as (
    select *,
        case when min_pitches < 200 then 100
             when min_pitches < 400 then 200
             when min_pitches < 800 then 400
             else 800 end as min_pitches_lo
    from pairs
)
select
    metric,
    min_pitches_lo,
    case min_pitches_lo when 100 then 199 when 200 then 399 when 400 then 799 end as min_pitches_hi,
    count(*)        as n_pairs,
    corr(v1, v2)    as yoy_corr_raw,
    corr(w1, w2)    as yoy_corr_within_type
from binned
group by metric, min_pitches_lo
