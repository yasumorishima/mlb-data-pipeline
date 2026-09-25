-- K% and BB% must be over PA (batters) and BF (pitchers). A 0-1 range test
-- only notices K% over AB on the 56 rows with AB = 0 (SO/AB never exceeds 1
-- otherwise), so check that the rate times its denominator gives back the
-- count. NULL rates fail too (the rows are already limited to PA/BF > 0).
select 'batter' as side, m.player_id, m.season
from {{ ref("mart_batter_season") }} m
join {{ ref("stg_statsapi__batting") }} b using (player_id, season)
where m.k_rate is null or m.bb_rate is null
   or abs(m.k_rate * b.pa - b.so) > 1e-6
   or abs(m.bb_rate * b.pa - b.bb) > 1e-6
union all
select 'pitcher', m.player_id, m.season
from {{ ref("mart_pitcher_season") }} m
join {{ ref("stg_statsapi__pitching") }} p using (player_id, season)
where p.bf > 0
  and (m.k_rate is null or m.bb_rate is null
    or abs(m.k_rate * p.bf - p.so) > 1e-6
    or abs(m.bb_rate * p.bf - p.bb) > 1e-6)
