select
    player_id,
    season,
    pa,
    bip,
    woba,
    est_woba                               as xwoba,
    est_ba                                 as xba,
    est_slg                                as xslg
from {{ source("raw", "sc_batter_expected") }}
