# 2026 MLB team wins: result of the pre-registered test

Write-up: [JP](https://zenn.dev/shogaku/articles/mlb-team-wins-forecast-2026) / [EN](https://dev.to/yasumorishima/how-well-do-player-projections-predict-team-wins-i-froze-mlb-2026-first-then-checked-3ih1)

Predictions frozen in `pred_2026.csv` (master `9e6edbb`, merged
2026-09-30 00:40 UTC) before the 2026 standings were fetched. At scoring
time the md5 of all seven frozen files and of the two input tables
matched `PREREG.md`. Teams played
161 or 162 games. Run: `python fetch.py --standings 2015-2026` then
`python score.py --2026`.

## Result

| arm | MAE (wins) |
| --- | ---: |
| model (Marcel player projections, primary) | 7.92 |
| stretched (secondary) | 8.16 |
| every team .500 | 8.50 |
| last season's win share | 8.83 |
| last season's Pythagenpat | 8.71 |

Differences in MAE, model minus floor, 95 % interval from resampling the
30 teams:

| vs | difference | 95 % interval | pre-registered verdict |
| --- | ---: | --- | --- |
| .500 | -0.58 | -2.43 to +1.12 | indistinguishable |
| last season | -0.91 | -2.98 to +1.22 | indistinguishable |
| last season's Pythagenpat | -0.80 | -2.82 to +1.36 | indistinguishable |
| stretched (H2; model minus stretched) | -0.24 | -0.71 to +0.27 | indistinguishable |

- **H1: indistinguishable from all three floors.** The model is lower on
  the point estimate each time, but one season of 30 teams cannot tell a
  difference of this size apart, and the pre-registered rule does not read
  it as a win.
- **H2: indistinguishable**; the stretched arm is worse on the point
  estimate (8.16 against 7.92).
- Correlation of projected and actual wins: 0.48 (0.63 over the backtest).
- Not judged, for context: the standard deviation of wins was 11.0 in
  2026 (10.7 to 15.9 in the backtest seasons; 10.7 in 2016 and 11.5 in
  2017), and the .500 floor scored 8.50 (9.03 to 13.13 in the backtest
  seasons, 10.60 on average).
- Largest misses: MIL 103 wins (projected 83.7), TB 98 (80.0), SF 65
  (82.9), ATH 64 (79.8), SD 91 (76.6).

Per-season check of the backtest, for comparison: the model beat last
season's Pythagenpat in 7 of 9 seasons, by -1.07 on average. That backtest
is in-sample for the design choices, and its interval against this floor
only just cleared zero (upper end -0.05).
