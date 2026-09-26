-- Dashboard table for pitchers, same idea as bi_batter_season. Percentiles
-- are among pitchers with BF >= 30% of the season's BF leader (about 250 BF
-- in a full season, so full-time relievers are ranked too).
-- role: SP when at least half the pitcher's games were starts; NULL when
-- starts are unknown rather than a silent 'RP'.
with m as (
    select *, bf >= 0.3 * max(bf) over (partition by season) as pctile_qualified
    from {{ ref("mart_pitcher_season") }}
)
select
    m.player_id,
    m.season,
    m.player_name,
    t.team_abbr,
    t.team_name,
    t.league,
    t.division,
    case when m.games_started is null or m.games is null or m.games = 0 then null
         when 2 * m.games_started >= m.games then 'SP' else 'RP' end           as role,
    m.age,
    m.games,
    m.games_started,
    m.ip,
    m.bf,
    m.era,
    m.fip,
    m.xera,
    m.era_minus_xera,
    m.k_rate,
    m.bb_rate,
    m.k_minus_bb_rate,
    m.xwoba_allowed,
    m.primary_pitch_type,
    m.war,
    m.is_partial,
    m.pctile_qualified,
    {{ season_pctile("m.k_rate", "m.pctile_qualified") }}                               as k_rate_pctile,
    {{ season_pctile("m.bb_rate", "m.pctile_qualified", lower_is_better=true) }}        as bb_rate_pctile,
    {{ season_pctile("m.k_minus_bb_rate", "m.pctile_qualified") }}                      as k_minus_bb_pctile,
    {{ season_pctile("m.fip", "m.pctile_qualified", lower_is_better=true) }}            as fip_pctile,
    {{ season_pctile("m.xera", "m.pctile_qualified", lower_is_better=true) }}           as xera_pctile,
    {{ season_pctile("m.xwoba_allowed", "m.pctile_qualified", lower_is_better=true) }}  as xwoba_allowed_pctile
from m
left join {{ ref("mlb_teams") }} t on t.team_id = m.team_id
