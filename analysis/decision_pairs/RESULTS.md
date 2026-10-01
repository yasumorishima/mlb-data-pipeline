# Results: two pitchers, pick one (2025 -> 2026)

Pre-registration: `PREREG.md` (frozen in #45, test revision named in #47
before the run). Run 2026-10-01 on HF `yasumorishima/mlb-stats` revision
`e006cbe12da75aac0cb96892ac8b43b9a27e5261`, `pairs.py` md5
`684a2ff742203bb37f8a3c141d87b4aa`, output `primary.json` (md5
`c5268ea9d823bf52ec581b1db1fb44c2`).

## The question

Two pitchers in the same role are available and one has to be chosen on
last season's numbers alone. Which number most often picks the one who
allows fewer earned runs per nine innings the next season?

## The test: 2025 numbers, 2026 outcome

359 pitchers (at least 100 batters faced in both 2025 and 2026), 32,122
same-role pairs.

| rule (2025) | picked right | minus ERA | 95 % interval | reading |
|---|---|---|---|---|
| ERA | 0.5585 | | | |
| FIP | 0.5707 | +0.012 | −0.017 to +0.042 | indistinguishable |
| xERA | 0.5806 | +0.022 | −0.008 to +0.055 | indistinguishable |
| K−BB% | 0.5841 | +0.026 | −0.011 to +0.064 | indistinguishable |
| blend | 0.5849 | +0.026 | −0.010 to +0.065 | indistinguishable |

By the rule written before the run, none of the four is shown to beat
ERA on 2026 alone: every interval includes 0. The point estimates all
lie on the same side as in the development seasons, and the intervals
are about three times as wide as there, as the pre-registration expected
for one season. "Indistinguishable" is not read as "no difference".

## Next to the development seasons

| rule | 2015–2024 (259,065 pairs) | 2025 → 2026 (32,122 pairs) |
|---|---|---|
| ERA | 0.558 | 0.558 |
| FIP | 0.587 | 0.571 |
| xERA | 0.590 | 0.581 |
| K−BB% | 0.595 | 0.584 |
| blend | 0.599 (in-sample) | 0.585 |

The ERA rule lands on the same 0.558. The other four are 0.9 to 1.6
points lower than in development.

## Where the choice of rule matters

In the pairs where ERA and another rule pick different pitchers:

| pairs where ERA and … disagree | pairs | ERA right | the other rule right |
|---|---|---|---|
| FIP | 8,454 | 0.477 | 0.523 |
| xERA | 8,558 | 0.459 | 0.541 |
| K−BB% | 11,028 | 0.463 | 0.537 |
| blend | 10,766 | 0.461 | 0.539 |

In development the K−BB% figure was 0.555 against 0.445.

## What a pick is worth

Mean 2026 ERA of the pitcher not picked minus the one picked: ERA rule
0.23, K−BB% 0.37, blend 0.37 runs per nine innings (development: 0.28,
0.44, 0.46). In the pairs where ERA and K−BB% disagree, the pitcher
picked by ERA had the 2026 ERA 0.20 higher on average.

## By sample size (reported, not tested)

Bands by the smaller of the two pitchers' 2025 batters faced. The
pre-registration lists these as reported only, so no reading is drawn from
them; the intervals are the same pitcher resampling.

| 2025 batters faced | pairs | ERA | FIP | xERA | K−BB% | blend |
|---|---|---|---|---|---|---|
| 100–299 | 23,533 | 0.555 | 0.555 | 0.567 | 0.585 | 0.577 |
| 300–599 | 6,040 | 0.556 | 0.598 | 0.600 | 0.573 | 0.589 |
| 600+ | 2,549 | 0.598 | 0.648 | 0.666 | 0.607 | 0.647 |

The only interval above 0 is xERA in the 600+ band (+0.068, +0.008 to
+0.126). In development the 600+ band was the one where xERA's interval
crossed 0 (−0.003 to +0.040), so the two seasons do not agree on it.

## Sensitivity: a 50-batter bar in 2026

Whether a pitcher is scored depends on the team keeping him (see the
selection note in `PREREG.md`). With 50 batters instead of 100 in 2026
(39,555 pairs): ERA 0.564, FIP 0.577, xERA 0.589, K−BB% 0.588, blend
0.592. Every rule is slightly higher; xERA and K−BB% swap places (0.581
and 0.584 at the 100-batter bar).

## Material for the decision

- One season's ERA picks the better pitcher of two about 56 % of the
  time, in 2026 as in 2015–2024. Every rule here is closer to a coin flip
  than to certainty.
- FIP, xERA, K−BB% and the blend picked right more often than ERA in
  2026 too (by 1.2 to 2.6 points), but 2026 alone cannot separate them
  from ERA. The case for them rests on the development seasons, where the
  gap was 2.9 to 4.0 points with intervals clear of 0 (the blend's
  in-sample).
- When ERA and K−BB% point at different pitchers, about a third of all
  pairs, K−BB% was right 54 % of the time in 2026 and 56 % in 2015–2024.
- Not covered: cost, contract, health, role changes, park, and anything
  scouts see that these four numbers do not. The decision set only
  contains pitchers who reached 100 batters the next season.
