-- Dashboard table for pitch-level scouting: mart_pitch_arsenal_scouting with
-- the pitcher's name and team, and its within-type percentiles on the same
-- 0-100 scale as the season tables (NULL below 100 pitches, as in the mart).
select
    a.player_id,
    a.season,
    p.player_name,
    t.team_abbr,
    a.pitch_type,
    a.pitch_name,
    a.pitches,
    a.usage,
    a.usage_rank,
    a.run_value_per_100,
    a.whiff_rate,
    a.xwoba_allowed,
    a.hard_hit_rate,
    cast(round(100 * a.whiff_pctile_in_type) as {{ dbt.type_bigint() }}) as whiff_pctile_in_type,
    cast(round(100 * a.rv100_pctile_in_type) as {{ dbt.type_bigint() }}) as rv100_pctile_in_type
from {{ ref("mart_pitch_arsenal_scouting") }} a
left join {{ ref("mart_pitcher_season") }} p
    on p.player_id = a.player_id and p.season = a.season
left join {{ ref("mlb_teams") }} t on t.team_id = p.team_id
