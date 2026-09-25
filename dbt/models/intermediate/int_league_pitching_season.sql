-- League-wide totals per season and the FIP constant derived from them:
--   cFIP = lgERA - (13*HR + 3*(BB+HBP) - 2*K) / IP
-- (the FanGraphs definition, with BB including intentional walks).
with totals as (
    select
        season,
        sum(outs) / 3.0                        as ip,
        sum(er)                                as er,
        sum(hr)                                as hr,
        sum(bb)                                as bb,
        sum(hbp)                               as hbp,
        sum(so)                                as so,
        sum(bf)                                as bf
    from {{ ref("stg_statsapi__pitching") }}
    group by season
)
select
    season,
    ip,
    bf,
    9.0 * er / ip                              as lg_era,
    so::double / bf                            as lg_k_rate,
    bb::double / bf                            as lg_bb_rate,
    9.0 * er / ip
      - (13.0 * hr + 3.0 * (bb + hbp) - 2.0 * so) / ip as fip_constant
from totals
