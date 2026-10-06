-- Dashboard table for "which numbers carry over to next season": the four
-- reliability marts in one long table, one row per area, metric and group.
-- A group is a sample-size bin (the smaller of the two seasons' PA, BF,
-- pitches or competitive runs) or, for fielding, a position.
-- yoy_corr is the same metric in consecutive seasons; for pitch types it is
-- the within-type correlation (yoy_corr_raw keeps the raw one). next_corr is
-- this season's metric against next season's wOBA (batting) or ERA
-- (pitching); for ERA lower is better, so strikeout rates come out negative
-- and next_corr_strength is its absolute value. mean_change is next season
-- minus this season (fielding and running only).
-- metric_label and group_label have no fallback: a metric or position added
-- to a mart without a mapping here fails the not_null contract instead of
-- showing a raw column name or a blank group.
with batting as (
    select 'Batting' as area, metric, 'PA' as sample_unit,
           min_pa_lo as group_lo, min_pa_hi as group_hi,
           n_pairs, yoy_corr,
           cast(null as {{ float_type() }}) as yoy_corr_raw,
           'wOBA' as next_target, next_woba_corr as next_corr,
           cast(null as {{ float_type() }}) as mean_change,
           cast(null as {{ dbt.type_string() }}) as position
    from {{ ref("mart_batter_process_reliability") }}
),
pitching as (
    select 'Pitching', metric, 'BF',
           min_bf_lo, min_bf_hi,
           n_pairs, yoy_corr,
           cast(null as {{ float_type() }}),
           'ERA', next_era_corr,
           cast(null as {{ float_type() }}),
           cast(null as {{ dbt.type_string() }})
    from {{ ref("mart_pitcher_process_reliability") }}
),
pitch_type as (
    select 'Pitch type', metric, 'pitches',
           min_pitches_lo, min_pitches_hi,
           n_pairs, yoy_corr_within_type,
           yoy_corr_raw,
           cast(null as {{ dbt.type_string() }}), cast(null as {{ float_type() }}),
           cast(null as {{ float_type() }}),
           cast(null as {{ dbt.type_string() }})
    from {{ ref("mart_scouting_reliability") }}
),
fielding_running as (
    select case when metric in ('sprint_speed', 'hp_to_1b') then 'Running' else 'Fielding' end,
           metric,
           case when metric in ('sprint_speed', 'hp_to_1b') then 'competitive runs' else 'position' end,
           min_runs_lo, min_runs_hi,
           n_pairs, yoy_corr,
           cast(null as {{ float_type() }}),
           cast(null as {{ dbt.type_string() }}), cast(null as {{ float_type() }}),
           mean_change,
           case when min_runs_lo is null then group_label end
    from {{ ref("mart_fielding_running_reliability") }}
),
allr as (
    select * from batting
    union all select * from pitching
    union all select * from pitch_type
    union all select * from fielding_running
)
select
    area,
    metric,
    case
        when area = 'Batting' then case metric
            when 'woba' then 'wOBA' when 'xwoba' then 'xwOBA'
            when 'woba_minus_xwoba' then 'wOBA - xwOBA'
            when 'k_rate' then 'K%' when 'bb_rate' then 'BB%'
            when 'iso' then 'ISO' when 'babip' then 'BABIP' end
        when area = 'Pitching' then case metric
            when 'era' then 'ERA' when 'fip' then 'FIP' when 'xera' then 'xERA'
            when 'era_minus_xera' then 'ERA - xERA'
            when 'k_minus_bb_rate' then 'K-BB%' when 'k_rate' then 'K%' when 'bb_rate' then 'BB%'
            when 'xwoba_allowed' then 'xwOBA allowed' end
        when area = 'Pitch type' then case metric
            when 'whiff_rate' then 'Whiff%' when 'run_value_per_100' then 'Run value per 100'
            when 'xwoba_allowed' then 'xwOBA allowed' when 'hard_hit_rate' then 'Hard-hit%'
            when 'usage' then 'Usage' end
        when area = 'Fielding' then case metric
            when 'oaa' then 'OAA' when 'fielding_runs_prevented' then 'Fielding runs prevented'
            when 'success_rate_diff' then 'Catch rate vs expected' end
        when area = 'Running' then case metric
            when 'sprint_speed' then 'Sprint speed' when 'hp_to_1b' then 'Home to first' end
    end                                                               as metric_label,
    sample_unit,
    case
        when position is not null then position
        -- Explicit: DuckDB's concat skips a NULL argument (BigQuery's returns NULL).
        when group_lo is null then null
        when group_hi is null then concat(cast(group_lo as {{ dbt.type_string() }}), '+')
        else concat(cast(group_lo as {{ dbt.type_string() }}), '-', cast(group_hi as {{ dbt.type_string() }}))
    end                                                               as group_label,
    -- Sorts bins by size and positions by their scorebook number.
    cast(case position
        when '1B' then 3 when '2B' then 4 when '3B' then 5 when 'SS' then 6
        when 'LF' then 7 when 'CF' then 8 when 'RF' then 9
        else group_lo end as {{ dbt.type_bigint() }})                  as group_order,
    cast(group_lo as {{ dbt.type_bigint() }})                          as group_lo,
    cast(group_hi as {{ dbt.type_bigint() }})                          as group_hi,
    cast(n_pairs as {{ dbt.type_bigint() }})                           as n_pairs,
    cast(yoy_corr as {{ float_type() }})                           as yoy_corr,
    cast(yoy_corr_raw as {{ float_type() }})                       as yoy_corr_raw,
    next_target,
    cast(next_corr as {{ float_type() }})                          as next_corr,
    cast(abs(next_corr) as {{ float_type() }})                     as next_corr_strength,
    cast(mean_change as {{ float_type() }})                        as mean_change
from allr
