-- Dashboard table for batters: the columns a scout or coach reads, the
-- franchise's current name, and season percentiles (100 = best) in the style
-- of Baseball Savant's percentile rankings.
-- Percentiles are among batters with PA >= 40% of the season's PA leader, a
-- bar that scales with the 60-game 2020 season and with a season in progress
-- without needing a schedule table (about 300 PA in a full season).
with m as (
    select *, pa >= 0.4 * max(pa) over (partition by season) as pctile_qualified
    from {{ ref("mart_batter_season") }}
)
select
    m.player_id,
    m.season,
    m.player_name,
    t.team_abbr,
    t.team_name,
    t.league,
    t.division,
    m.primary_position,
    m.age,
    m.games,
    m.pa,
    m.avg,
    m.obp,
    m.slg,
    m.woba,
    m.xwoba,
    m.woba_minus_xwoba,
    m.wrc_plus,
    m.war,
    m.k_rate,
    m.bb_rate,
    m.iso,
    m.avg_bat_speed,
    m.sprint_speed,
    m.oaa,
    m.is_partial,
    m.pctile_qualified,
    {{ season_pctile("m.woba", "m.pctile_qualified") }}                         as woba_pctile,
    {{ season_pctile("m.xwoba", "m.pctile_qualified") }}                        as xwoba_pctile,
    {{ season_pctile("m.wrc_plus", "m.pctile_qualified") }}                     as wrc_plus_pctile,
    {{ season_pctile("m.k_rate", "m.pctile_qualified", lower_is_better=true) }} as k_rate_pctile,
    {{ season_pctile("m.bb_rate", "m.pctile_qualified") }}                      as bb_rate_pctile,
    {{ season_pctile("m.iso", "m.pctile_qualified") }}                          as iso_pctile,
    {{ season_pctile("m.avg_bat_speed", "m.pctile_qualified") }}                as bat_speed_pctile,
    {{ season_pctile("m.sprint_speed", "m.pctile_qualified") }}                 as sprint_speed_pctile,
    {{ season_pctile("m.oaa", "m.pctile_qualified") }}                          as oaa_pctile
from m
left join {{ ref("mlb_teams") }} t on t.team_id = m.team_id
