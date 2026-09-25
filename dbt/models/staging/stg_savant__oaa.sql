select
    player_id,
    season,
    position                               as position_code,
    primary_pos_formatted                  as primary_position,
    outs_above_average                     as oaa,
    fielding_runs_prevented
from {{ source("raw", "oaa") }}
