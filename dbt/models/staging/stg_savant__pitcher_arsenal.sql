select
    player_id,
    season,
    pitch_type,
    pitch_name,
    pitches,
    pitch_usage / 100.0                    as usage,
    run_value,
    run_value_per_100,
    whiff_percent / 100.0                  as whiff_rate,
    k_percent / 100.0                      as k_rate,
    est_woba                               as xwoba_allowed,
    hard_hit_percent / 100.0               as hard_hit_rate
from {{ source("raw", "sc_pitcher_arsenal") }}
