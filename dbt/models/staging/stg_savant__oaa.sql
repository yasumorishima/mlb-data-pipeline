select
    player_id,
    season,
    position                               as position_code,
    primary_pos_formatted                  as primary_position,
    outs_above_average                     as oaa,
    fielding_runs_prevented,
    -- Savant prints it as '4%' / '-3%' (whole percentage points). A plain cast,
    -- so a change of format fails the build instead of turning into NULLs.
    cast(replace(diff_success_rate_formatted, '%', '') as {{ dbt.type_int() }}) as success_rate_diff
from {{ source("raw", "oaa") }}
