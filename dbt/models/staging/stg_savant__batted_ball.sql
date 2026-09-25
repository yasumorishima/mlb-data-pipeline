select
    id                                     as player_id,
    season,
    bbe,
    gb_rate,
    fb_rate,
    ld_rate,
    pu_rate,
    pull_rate,
    oppo_rate,
    pull_air_rate
from {{ source("raw", "sc_batted_ball") }}
