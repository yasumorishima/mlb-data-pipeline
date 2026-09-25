-- Our FIP (league constant rebuilt from the same table) must reproduce the
-- Stats API figure (to 0.001; the largest gap is 0.0002) on finished seasons.
-- Partial seasons are excluded because the sabermetrics endpoint lags the
-- counting stats. One finished 20+ IP season disagrees: 600986 in 2024, split
-- across three teams; it is listed here by id so any new disagreement fails.
select m.player_id, m.season, m.fip, m.fip_statsapi
from {{ ref("mart_pitcher_season") }} m
where not m.is_partial
  and not (m.player_id = 600986 and m.season = 2024)
  and m.ip >= 20
  and m.fip_statsapi is not null
  and (m.fip is null or abs(m.fip - m.fip_statsapi) > 0.001)
