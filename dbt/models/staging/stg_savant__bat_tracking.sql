select
    id                                     as player_id,
    season,
    competitive_swings,
    avg_bat_speed,
    swing_tilt,
    attack_angle,
    ideal_attack_angle_rate
from {{ source("raw", "sc_bat_tracking") }}
