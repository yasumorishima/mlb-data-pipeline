-- ISO must be (TB - H) / AB from counts, not SLG - AVG from the rounded
-- 3-decimal rates, and NULL when AB = 0.
select m.player_id, m.season, m.iso
from {{ ref("mart_batter_season") }} m
join {{ ref("stg_statsapi__batting") }} b using (player_id, season)
where (b.ab = 0 and m.iso is not null)
   or (b.ab > 0 and (m.iso is null or abs(m.iso * b.ab - (b.tb - b.hits)) > 1e-6))
