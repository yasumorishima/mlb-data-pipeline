# Pre-registration: two hitters, pick one

The hitter version of `../decision_pairs` (two pitchers, pick one; that
test came out indistinguishable for all four rules, see its `RESULTS.md`).
Two hitters are available and one has to be chosen on last season's
numbers. Which number picks the one with the higher wOBA next season, and
how often is it right?

Written before any 2026 hitter outcome was read by this code or by me.
The 2026 regular season ended on 2026-09-27. The single revision used
here, `e006cbe12da75aac0cb96892ac8b43b9a27e5261`, was built on 2026-10-01
from a fetch made after the last game and already holds the final 2026
rows; the development run reads t <= 2024 only (outcomes up to 2025), and
"not read" for 2026 rests on that and on my not looking. From that
revision I have read the 2026 pitcher outcomes (the pitcher test), the
count of hitter rows with 100+ PA by position over all seasons including
2026, and the counts per season up to 2025; no 2026 hitter statistic. The marts built from it also contain
`mart_batter_process_reliability` with 2025 -> 2026 pairs; I have not
read it.

## The decision

- A hitter-season enters in season t when he had at least 100 plate
  appearances in t and in t+1, and has wOBA, xwOBA, wRC+, K%, BB% and ISO
  in t and a wOBA in t+1. All positions together (no row with 100+ PA has
  primary position P).
- Every pair of such hitters in the same season t is one decision. A pair
  whose two t+1 wOBAs are equal is dropped.
- A rule picks the hitter with the higher wOBA, xwOBA or wRC+ in season t.
  A tie in the rule counts as half right.
- The pick is right when the picked hitter has the higher wOBA in t+1.
  wOBA is not park-adjusted; wRC+ is defined as park- and league-adjusted
  (not checked on these rows). It is scored against the unadjusted wOBA on
  purpose: the question is who produces more.
- xwOBA comes rounded to 0.001 while wOBA is at full precision, so the
  xwOBA rule has more ties (about 0.7–0.8 % of pairs in two development
  seasons), each counting half right: a small handicap for H1, left as is.
- Hitters listed at primary position `X` stay in (50 development rows).
- `blend` is the least-squares fit of the t+1 wOBA on season-t wOBA,
  xwOBA, K%, BB% and ISO over the development rows; it picks the higher
  fitted value. Coefficients are frozen in `pairs.py` (`FROZEN_BETA`) and
  the script stops if the refit differs by more than 1e-9.
- Survivorship, as in the pitcher version: both hitters have to reach 100
  PA in t+1 for the pick to be scored, so the estimand is conditional on
  the teams' own playing-time choices. A 50-PA bar in t+1 is reported as a
  sensitivity.

## Development run (already seen, not the test)

t = 2015..2024 without t = 2019 and t = 2020, 2,792 hitter-seasons,
486,143 pairs. Intervals: 2,000 resamples of hitters (a hitter's rows in
every season move together, seed 0).

| rule | right | minus wOBA, 95 % |
|---|---|---|
| wOBA | 0.6379 | |
| xwOBA | 0.6584 | +0.012 to +0.029 |
| wRC+ | 0.6366 | −0.004 to +0.001 |
| blend | 0.6606 | +0.016 to +0.029 (in-sample) |

Blend: wOBA_{t+1} = 0.1786 − 0.0013·wOBA + 0.4014·xwOBA − 0.0475·K%
+ 0.0562·BB% + 0.0799·ISO.

In the 89,920 pairs where wOBA and xwOBA disagree, xwOBA is right 0.556.
In the 22,998 where wOBA and wRC+ disagree, wOBA is right 0.514.

By the smaller of the two hitters' PA in t: 100–299 wOBA 0.620 / xwOBA
0.647; 300–499 0.649 / 0.666; 500+ 0.669 / 0.676 (xwOBA interval −0.008
to +0.021). 50-PA sensitivity (563,073 pairs): wOBA 0.643, xwOBA 0.660,
wRC+ 0.641, blend 0.663.

## Test

`pairs.py --mode primary`: t = 2025, outcome = 2026 wOBA, same revision,
same code (md5 below). The script stops unless that revision's input
`statsapi_batting.parquet` was committed on or after 2026-09-29, the 2026
regular-season schedule has no game whose state is not Final, and no 2026
row used is marked `is_partial`. If it stops, or the blend refit differs
from `FROZEN_BETA`, there is no test result; the cause is recorded under
Amendments before anything is changed.

The freeze is the merge of this file and `pairs.py` into master; the
test runs only after that merge, and the merge commit is recorded in
`RESULTS.md`.

## Hypotheses and how they are read

Primary statistic: share of pairs picked right, all PA bands pooled,
t = 2025. Difference from the wOBA rule, 95 % interval from 2,000
resamples of hitters (seed 0).

- **H1 xwOBA**, **H2 wRC+**, **H3 blend** against wOBA. "Better" only if
  the whole interval is above 0, "worse" only if it is below 0, otherwise
  "indistinguishable". All three are reported; no multiplicity correction
  and no family-wise claim. The development seasons give no reason to
  expect wRC+ to differ from wOBA; H2 is there so that a park adjustment
  that hurts is visible.
- One season has about a ninth of the development pairs, so intervals
  roughly three times as wide are expected. "Indistinguishable" is not
  read as "no difference".
- Reported, not tested: the three PA bands, the pairs where wOBA and each
  other rule disagree, the 50-PA sensitivity, and the mean wOBA gap (next
  season wOBA of the hitter picked minus the one not picked; a tie in the
  rule adds 0 but stays in the denominator).

## Frozen

| file | md5 |
|---|---|
| `pairs.py` | `f4e488d3fca90da1fbf07016bad139a7` |
| `dev.json` | `29df9d07181de189ffabb0b998658276` |

`dev.json` was written by the same code before `FROZEN_BETA` was filled
in from it and before the two test-only checks below were added; none of
those touch the development path (an audit re-ran it with the frozen
blend on a frame holding only seasons up to 2025 and got the same JSON).

## Audit before the freeze

An opus audit reproduced two development seasons (55,277 and 63,901
pairs) with an independent brute force to about 1e-15, found the frozen
blend equal to the refit, every number above equal to `dev.json`, and no
duplicate player-seasons. It read no 2026 hitter statistic. It asked for
the schedule check and the `is_partial` check the pitcher version has,
for the freeze to be a merge before the run, and for the xwOBA rounding,
the wRC+ definition and the `X` rows to be stated. All are done above.

## Amendments

(none)
