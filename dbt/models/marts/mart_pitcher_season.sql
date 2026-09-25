-- Pitcher evaluation, one row per player-season. Rates use batters faced.
with p as (select * from {{ ref("stg_statsapi__pitching") }}),
lg as (select * from {{ ref("int_league_pitching_season") }}),
x as (select * from {{ ref("stg_savant__pitcher_expected") }}),
arsenal as (
    select
        player_id,
        season,
        count(*) filter (where usage >= 0.05)  as n_pitch_types_5pct,
        arg_max(pitch_type, usage)             as primary_pitch_type,
        max(usage)                             as primary_pitch_usage
    from {{ ref("stg_savant__pitcher_arsenal") }}
    group by player_id, season
)
select
    p.player_id,
    p.season,
    p.player_name,
    p.team_id,
    p.age,
    p.games,
    p.games_started,
    p.ip,
    p.bf,
    p.so::double / nullif(p.bf, 0)             as k_rate,
    p.bb::double / nullif(p.bf, 0)             as bb_rate,
    (p.so - p.bb)::double / nullif(p.bf, 0)    as k_minus_bb_rate,
    p.go::double / nullif(p.go + p.ao, 0)      as ground_out_share,
    p.era,
    (13.0 * p.hr + 3.0 * (p.bb + p.hbp) - 2.0 * p.so) / nullif(p.ip, 0)
        + lg.fip_constant                      as fip,
    p.fip_statsapi,
    x.xera,
    p.era - x.xera                             as era_minus_xera,
    x.xwoba_allowed,
    a.n_pitch_types_5pct,
    a.primary_pitch_type,
    a.primary_pitch_usage,
    p.war,
    p.is_partial
from p
join lg using (season)
left join x using (player_id, season)
left join arsenal a using (player_id, season)
