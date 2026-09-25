-- Companion to assert_pa_identity: surfaces the +/-1 rows as a warning so a
-- sudden jump in their count is visible.
{{ config(severity="warn") }}
select player_id, season, pa, ab + bb + hbp + sf + sh + ci as pa_from_parts
from {{ ref("stg_statsapi__batting") }}
where abs(pa - (ab + bb + hbp + sf + sh + ci)) = 1
