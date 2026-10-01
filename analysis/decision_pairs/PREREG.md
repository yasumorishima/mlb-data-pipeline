# Pre-registration: two pitchers, pick one

The question is a decision, not a correlation. Two pitchers in the same
role are available and one has to be chosen on last season's numbers. Which
number picks the one who allows fewer earned runs next season, and how often
is it right?

Written before any 2026 outcome was read by this code or by me. The 2026
regular season ended on 2026-09-27, so the 2026 ERAs exist in the world;
this guards against tuning on the 2026 result, not against knowing it.
The development revision below already holds 2026 rows (fetched
2026-09-27 17:21 UTC, before that day's games ended), and `pairs.py` loads
that file in both modes; nothing in the code keeps it from reading them, so
"not read" rests on the development run using t <= 2024 only and on my
not looking. The `is_partial` column cannot gate the test: it is a
calendar flag (`season >= the fetch year`), so it stays true for 2026 until
2027.

## The decision

- A pitcher-season enters in season t when he faced at least 100 batters in
  t and at least 100 in t+1, and has ERA, FIP, xERA and K−BB% in t and an
  ERA in t+1. Role is starter when `games_started / games >= 0.5` in t,
  reliever otherwise.
- Every pair of such pitchers with the same season t and the same role is
  one decision. A pair whose two t+1 ERAs are equal is dropped.
- A rule picks the pitcher with the lower ERA / FIP / xERA, or the higher
  K−BB%, in season t. A tie in the rule counts as half right.
- The pick is right when the picked pitcher has the lower ERA in t+1.
- The fifth rule, `blend`, is the least-squares fit of the t+1 ERA on the
  four season-t numbers over the development rows (coefficients below) and
  picks the lower fitted value. It is fitted on the development rows only.
- Survivorship is part of the question as posed: both pitchers have to
  pitch 100+ batters in t+1 for the pick to be scored, so the estimand is
  conditional on the teams' own choices. That selection is not neutral
  between the rules: in the development seasons the share reaching 100
  batters the next year falls with ERA (quartiles 0.82 / 0.76 / 0.69 /
  0.51) and is lower for pitchers whose ERA ran furthest above their FIP
  (ERA − FIP quartiles 0.68 / 0.75 / 0.73 / 0.62), which is the set of
  pairs where ERA and FIP disagree. The direction of the resulting bias on
  H1–H4 is not known. As a reported sensitivity, every run also scores the
  pairs with a 50-batter bar in t+1.

## Development run (already seen, not the test)

HF `yasumorishima/mlb-stats` revision `07a4d4f991b8553443173d8e119e12740a22ae3a`,
t = 2015..2024 without t = 2019 and t = 2020 (the 60-game season on either
side), 2,883 pitcher-seasons, 259,065 pairs. Intervals: 2,000 resamples of
pitchers (a pitcher's rows in every season move together, seed 0).

| rule | right | minus ERA, 95 % |
|---|---|---|
| ERA | 0.5584 | |
| FIP | 0.5871 | +0.018 to +0.039 |
| xERA | 0.5896 | +0.021 to +0.041 |
| K−BB% | 0.5946 | +0.023 to +0.049 |
| blend | 0.5987 | +0.028 to +0.052 (in-sample) |

Blend: ERA_{t+1} = 4.1059 − 0.0472·ERA + 0.0254·FIP + 0.2220·xERA − 4.8263·K−BB%.

In the pairs where ERA and K−BB% disagree (84,598), K−BB% is right 0.555
and ERA 0.445.

## Test

t = 2025, outcome = 2026 ERA. The code, the thresholds and the blend
coefficients are the ones in this commit (md5 of `pairs.py` below).

- The season-2025 numbers (ERA, FIP, xERA, K−BB%, batters faced, role) are
  read from `DEV_REVISION`, the revision above, so a later re-model of xERA
  cannot change the picks. The blend refitted on the development rows must
  match the frozen coefficients within 1e-9 or the script stops.
- Only the 2026 ERA and batters faced come from the test revision. The test
  revision is the first `marts` commit on HF built after this file is
  merged whose input `statsapi_pitching.parquet` was committed on or after
  2026-09-29 00:00 UTC. It is the one written by the refresh I dispatch
  right after the merge, and its sha is added under Amendments before the
  run. `pairs.py --mode primary` itself stops unless that file date holds
  and the 2026 regular-season schedule has no game whose state is not
  Final (checked 2026-10-01: 2,459 schedule entries, all Final, 28 of them with the detailed state Postponed; the Yankees and
  Orioles played 161 after the 2026-09-27 game was cancelled).

## Hypotheses and how they are read

Primary statistic: share of pairs picked right, all bands pooled, t = 2025.
For each of FIP, xERA, K−BB% and blend: the difference from the ERA rule,
95 % interval from 2,000 resamples of pitchers (seed 0).

- **H1 FIP, H2 xERA, H3 K−BB%, H4 blend** beat ERA. "Better" only if the
  whole interval is above 0, "worse" only if it is below 0, otherwise
  "indistinguishable". Four readings are reported; none is dropped. There
  is no multiplicity correction and no family-wise claim: each reading
  stands on its own, and the four are strongly correlated.
- One season has about a tenth of the development pairs, so an interval
  roughly three times as wide as the development one is expected. An
  "indistinguishable" reading is not read as "no difference".
- Reported, not tested: the three batter-faced bands (the smaller of the
  two pitchers' t batters faced: 100–299, 300–599, 600+), the pairs where
  ERA and each other rule disagree, the 50-batter sensitivity, and the mean
  ERA gap (ERA of the pitcher not picked minus the one picked, next
  season; a tie in the rule adds 0 to the gap but stays in the
  denominator).

## Frozen

| file | md5 |
|---|---|
| `pairs.py` | `684a2ff742203bb37f8a3c141d87b4aa` |
| `dev.json` | `b4c2bf9f0e25cbe23863b816cf38e48e` |

The 50-batter sensitivity on the development seasons (319,534 pairs):
ERA 0.5613, FIP 0.5887, xERA 0.5899, K−BB% 0.5976, blend 0.6005.

## Audit before the freeze

An opus audit of the first draft reproduced the pair counts and all five
rules on two cells with an independent brute force (to 10 decimals) and
found no error in the pair logic or the bootstrap. It found that the draft
gated on `is_partial` (which cannot turn false before 2027), left the test
revision free and let the season-2025 numbers come from it, and did not
state the selection. Those three are fixed above.

## Amendments

1. 2026-10-01, before the test run. The test revision is
   `e006cbe12da75aac0cb96892ac8b43b9a27e5261` (marts built by pipeline
   commit `67512dd`, input dataset revision `e5f1156`, whose
   `statsapi_pitching.parquet` was committed 2026-10-01 00:47:59 UTC). It is
   the first marts commit built after this file was merged with a statsapi
   fetch made after 2026-09-29; the 2026-09-28 scheduled refresh failed and
   wrote nothing (its end-year probe lost the finished season, fixed in
   PR #46). Only the 2026 row count and `is_partial` were read from it
   (868 rows, none partial), no ERA.
2. PR #46 also made `is_partial` false once a season's regular-season
   schedule is all Final, so the statements above that it "stays true for
   2026 until 2027" and "cannot turn false before 2027" no longer hold for
   new revisions. `pairs.py` does not gate on it in the test, so the frozen
   code and its md5 are unchanged.
