-- Aliased: on BigQuery a bare sprint_speed would name the table (a STRUCT of the row), not the column.
select
    player_id,
    season,
    competitive_runs,
    s.sprint_speed,
    hp_to_1b
from {{ source("raw", "sprint_speed") }} s
