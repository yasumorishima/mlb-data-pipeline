-- Pitch-level scouting view: each pitcher-season-pitch with its rank in the
-- pitcher's mix and its percentile among the same pitch type that season
-- (pitches >= 100 only, so percentiles are not driven by tiny samples).
-- Savant run value is from the pitcher's side (positive = good for the
-- pitcher; corr with wOBA allowed is -0.83), so 1.0 is the best pitch.
-- A pitch type thrown by a single qualifying pitcher that season (knuckleballs,
-- forkballs) gets NULL rather than a percentile of 0.
with a as (
    select * from {{ ref("stg_savant__pitcher_arsenal") }}
)
select
    player_id,
    season,
    pitch_type,
    pitch_name,
    pitches,
    usage,
    row_number() over (partition by player_id, season order by usage desc, pitch_type) as usage_rank,
    run_value_per_100,
    whiff_rate,
    xwoba_allowed,
    hard_hit_rate,
    case when pitches >= 100
          and count(*) over (partition by season, pitch_type, (pitches >= 100)) > 1 then
        percent_rank() over (partition by season, pitch_type, (pitches >= 100) order by whiff_rate)
    end                                        as whiff_pctile_in_type,
    case when pitches >= 100
          and count(*) over (partition by season, pitch_type, (pitches >= 100)) > 1 then
        percent_rank() over (partition by season, pitch_type, (pitches >= 100) order by run_value_per_100)
    end                                        as rv100_pctile_in_type
from a
