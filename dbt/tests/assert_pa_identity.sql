-- PA = AB + BB + HBP + SF + SH + CI. The Stats API is off by exactly +1 PA on
-- a handful of player-seasons (10 of 12,100 when this was written); those are
-- reported by assert_pa_identity_off_by_one as a warning, and a larger gap fails.
select player_id, season, pa, ab + bb + hbp + sf + sh + ci as pa_from_parts
from {{ ref("stg_statsapi__batting") }}
where abs(pa - (ab + bb + hbp + sf + sh + ci)) > 1
