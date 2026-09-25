-- Our FIP (league constant rebuilt from the same table) must reproduce the
-- Stats API figure exactly on finished, single-team seasons. Partial seasons
-- are excluded because the sabermetrics endpoint lags the counting stats, and
-- multi-team seasons because the API combines them differently.
select m.player_id, m.season, m.fip, m.fip_statsapi
from {{ ref("mart_pitcher_season") }} m
join {{ ref("stg_statsapi__pitching") }} s using (player_id, season)
where not m.is_partial
  and s.num_teams = 1
  and m.ip >= 20
  and m.fip_statsapi is not null
  and abs(m.fip - m.fip_statsapi) > 0.001
