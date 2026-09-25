select
    player_id,
    season,
    primary_pos_formatted                  as position,
    outs_above_average                     as oaa,
    fielding_runs_prevented
from {{ source("raw", "oaa") }}
