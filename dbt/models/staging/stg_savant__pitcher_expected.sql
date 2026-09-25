select
    player_id,
    season,
    pa,
    bip,
    woba                                   as woba_allowed,
    est_woba                               as xwoba_allowed,
    era,
    xera
from {{ source("raw", "sc_pitcher_expected") }}
