select
    player_id,
    season,
    competitive_runs,
    sprint_speed,
    hp_to_1b
from {{ source("raw", "sprint_speed") }}
