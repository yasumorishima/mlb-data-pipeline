-- Batter evaluation, one row per player-season with at least one PA.
-- Outcome stats (StatsAPI) sit next to process stats (Statcast) so the gap
-- between what happened and what the contact quality predicts is one column.
with b as (
    select * from {{ ref("stg_statsapi__batting") }} where pa > 0
),
oaa as (
    select player_id, season,
        -- sum() of BIGINT is HUGEINT in DuckDB, which parquet stores as DOUBLE
        cast(sum(oaa) as bigint) as oaa,
        cast(sum(fielding_runs_prevented) as bigint) as fielding_runs_prevented
    from {{ ref("stg_savant__oaa") }}
    group by player_id, season
)
select
    b.player_id,
    b.season,
    b.player_name,
    b.team_id,
    b.primary_position,
    b.age,
    b.games,
    b.pa,
    b.so::double / b.pa                        as k_rate,
    b.bb::double / b.pa                        as bb_rate,
    (b.tb - b.hits)::double / nullif(b.ab, 0)  as iso,
    b.avg,
    b.obp,
    b.slg,
    b.babip,
    b.woba,
    x.xwoba,
    b.woba - x.xwoba                           as woba_minus_xwoba,
    b.wrc_plus,
    b.war,
    bb_.gb_rate,
    bb_.fb_rate,
    bb_.ld_rate,
    bb_.pull_air_rate,
    bt.avg_bat_speed,
    bt.attack_angle,
    sp.sprint_speed,
    oaa.oaa,
    oaa.fielding_runs_prevented,
    b.is_partial
from b
left join {{ ref("stg_savant__batter_expected") }} x using (player_id, season)
left join {{ ref("stg_savant__batted_ball") }} bb_ using (player_id, season)
left join {{ ref("stg_savant__bat_tracking") }} bt using (player_id, season)
left join {{ ref("stg_savant__sprint_speed") }} sp using (player_id, season)
left join oaa using (player_id, season)
