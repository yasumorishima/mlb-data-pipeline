-- Consecutive-season pairs for the same batter: the input of a delta-method
-- or hierarchical aging curve. Both seasons need PA > 0; the harmonic mean of
-- the two PAs is the usual weight for a delta.
with s as (
    select player_id, season, age, pa, woba, k_rate, bb_rate, iso, is_partial
    from {{ ref("mart_batter_season") }}
)
select
    y1.player_id,
    y1.season                                  as season_1,
    y1.age                                     as age_1,
    y1.pa                                      as pa_1,
    y2.pa                                      as pa_2,
    2.0 / (1.0 / y1.pa + 1.0 / y2.pa)          as pair_weight,
    y1.woba                                    as woba_1,
    y2.woba                                    as woba_2,
    y2.woba - y1.woba                          as woba_delta,
    y2.k_rate - y1.k_rate                      as k_rate_delta,
    y2.bb_rate - y1.bb_rate                    as bb_rate_delta,
    y2.iso - y1.iso                            as iso_delta,
    (y1.season = 2020 or y2.season = 2020)     as touches_2020,
    y2.is_partial                              as second_season_partial
from s y1
join s y2
  on y2.player_id = y1.player_id
 and y2.season = y1.season + 1
