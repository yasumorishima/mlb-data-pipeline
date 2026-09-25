-- K% and BB% must be over PA (batters) and BF (pitchers). A range test cannot
-- tell PA from AB (SO/AB never exceeds 1 in this data), so check that the rate
-- times its denominator gives back the count.
select 'batter' as side, m.player_id, m.season
from {{ ref("mart_batter_season") }} m
join {{ ref("stg_statsapi__batting") }} b using (player_id, season)
where abs(m.k_rate * b.pa - b.so) > 1e-6
   or abs(m.bb_rate * b.pa - b.bb) > 1e-6
union all
select 'pitcher', m.player_id, m.season
from {{ ref("mart_pitcher_season") }} m
join {{ ref("stg_statsapi__pitching") }} p using (player_id, season)
where p.bf > 0
  and (abs(m.k_rate * p.bf - p.so) > 1e-6
    or abs(m.bb_rate * p.bf - p.bb) > 1e-6)
