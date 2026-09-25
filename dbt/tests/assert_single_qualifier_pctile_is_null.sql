-- A pitch type with a single qualifying pitcher (100+ pitches) in a season has
-- no one to be ranked against: its percentiles must be NULL, not 0.
with q as (
    select season, pitch_type, count(*) as n
    from {{ ref("mart_pitch_arsenal_scouting") }}
    where pitches >= 100
    group by season, pitch_type
)
select a.player_id, a.season, a.pitch_type
from {{ ref("mart_pitch_arsenal_scouting") }} a
join q using (season, pitch_type)
where a.pitches >= 100
  and q.n = 1
  and (a.whiff_pctile_in_type is not null or a.rv100_pctile_in_type is not null)
