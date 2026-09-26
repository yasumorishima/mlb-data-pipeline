-- Pitcher evaluation, one row per player-season. Rates use batters faced.
with p as (select * from {{ ref("stg_statsapi__pitching") }}),
lg as (select * from {{ ref("int_league_pitching_season") }}),
x as (select * from {{ ref("stg_savant__pitcher_expected") }}),
arsenal as (
    select
        player_id,
        season,
        count(case when usage >= 0.05 then 1 end) as n_pitch_types_5pct,
        max(usage)                             as primary_pitch_usage
    from {{ ref("stg_savant__pitcher_arsenal") }}
    group by player_id, season
),
-- The most used pitch; a tie goes to the first pitch code, the same order as
-- usage_rank in mart_pitch_arsenal_scouting (13 pitcher-seasons tie).
top_pitch as (
    select player_id, season, pitch_type as primary_pitch_type
    from (
        select player_id, season, pitch_type,
               row_number() over (partition by player_id, season order by usage desc, pitch_type) as rn
        from {{ ref("stg_savant__pitcher_arsenal") }}
    ) r
    where rn = 1
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
    cast(p.so as {{ float_type() }}) / nullif(p.bf, 0)             as k_rate,
    cast(p.bb as {{ float_type() }}) / nullif(p.bf, 0)             as bb_rate,
    cast((p.so - p.bb) as {{ float_type() }}) / nullif(p.bf, 0)    as k_minus_bb_rate,
    cast(p.go as {{ float_type() }}) / nullif(p.go + p.ao, 0)      as ground_out_share,
    p.era,
    (13.0 * p.hr + 3.0 * (p.bb + p.hbp) - 2.0 * p.so) / nullif(p.ip, 0)
        + lg.fip_constant                      as fip,
    p.fip_statsapi,
    x.xera,
    p.era - x.xera                             as era_minus_xera,
    x.xwoba_allowed,
    a.n_pitch_types_5pct,
    t.primary_pitch_type,
    a.primary_pitch_usage,
    p.war,
    p.is_partial
from p
join lg using (season)
left join x using (player_id, season)
left join arsenal a using (player_id, season)
left join top_pitch t using (player_id, season)
