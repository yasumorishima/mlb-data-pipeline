# Pre-registration: 2026 MLB team wins from player projections

Written before the 2026 final standings were read by this code or by me.
The 2026 regular season was already over (ended 2026-09-27) when this was
frozen, so this guards against tuning on the 2026 result, not against
knowing the world. The 2026 player stats are in the input tables;
`project.py` drops every row with `season >= Y` before projecting season Y.

## What the backtest is and is not

The 2016-2025 backtest (model MAE 8.27 against floors 10.60 / 9.67 / 9.34,
2020 not scored) was run while the design was being chosen (the NL pitcher
batting term, the runs-allowed renormalisation, the stretch arm), so it is
in-sample for those choices. Against the Pythagenpat floor its season-block
interval only just clears zero (upper end -0.05). 2026 is the first
out-of-sample test.

Known gaps, left as they are: players with no MLB history (rookies,
imports) get no projection; roster status is not filtered (players sent
down or on the injured list on opening day count); no fielding, park or
schedule; Ohtani is listed as P on the 2018-2019 rosters, so his batting is
missed in the 2019 backtest (he is TWP from 2020 on).

## What is frozen

- `fetch.py`, `project.py`, `score.py`, `freeze.py` at the commit that adds
  this file (md5 in the table below).
- `pred_2026.csv`: 30 teams, projected win share, `model_wins` and
  `stretched_wins` (both on 162 games).
- Inputs: `statsapi_batting.parquet` / `statsapi_pitching.parquet` from HF
  `yasumorishima/mlb-stats` (downloaded 2026-09-30; md5 `6e9a2f29f665d9fa68d9e68fbff41ed9`
  / `c3493a873104f486ce945d7d1c8f35a9`) and the opening-day 40-man rosters in
  `opening_rosters.parquet` (2026-03-25).

## Scoring (after the freeze)

1. `python fetch.py --standings 2015-2026` adds 2026 to `standings.parquet`.
2. If any team has not played all its games, wins are compared on win share
   x games played, as in the backtest (the same code path).
3. `python score.py` on 2026 only, plus the team-level comparison below.

## Hypotheses and decision rules

Primary statistic: mean absolute error of wins over the 30 teams, model
arm. Floors: every team .500; the 2025 win share; the 2025 Pythagenpat win
share (all x 2026 games, as in the backtest).

- **H1** model vs each floor. Difference in MAE (model minus floor) with a
  95 % interval from 20,000 resamples of the 30 teams (seed 0). "Better"
  only if the whole interval is below 0, "worse" only if it is above 0,
  otherwise "indistinguishable". One season, so the interval ignores the
  league-wide error that teams share; read it as optimistic.
- **H2** stretched vs model, same rule. Secondary.
- Also reported, not judged: correlation of projected and actual wins, the
  largest misses, and where 2026 sits against the backtest (model MAE 8.27,
  floors 10.60 / 9.67 / 9.34 over 2016-2025 without 2020).

No change to code, rosters, parameters or rules after the 2026 standings are
read. If something has to change, it goes in an AMENDMENTS section with the
date, before the standings are read, or it is reported as a deviation.

## Frozen files

| file | md5 |
| --- | --- |
| `fetch.py` | `3a136a50bbc4ecca9cc598c083f1873b` |
| `project.py` | `0a450ddbd36d2fdd72d49e1ca3556ce3` |
| `score.py` | `046f61817d4cf6fd39881e3d1a4f5c83` |
| `freeze.py` | `7b42f8af6a12dfef5533f934724db0e8` |
| `opening_rosters.parquet` | `c8e1cf59bd2ea3b6b4b1ebbfda23d028` |
| `projections_2016_2026.csv` | `31b1fe816da3c49bf121f26cb6d690c9` |
| `pred_2026.csv` | `922f558569c49743474d71c2808be6a1` |
