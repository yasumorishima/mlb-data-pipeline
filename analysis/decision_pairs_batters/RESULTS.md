# Results: two hitters, pick one (2025 -> 2026)

Write-up: [JP](https://qiita.com/ussu_ussu_ussu/items/280a6e3ff8357c27c8ec) / [EN](https://dev.to/yasumorishima/two-players-pick-one-on-last-seasons-numbers-how-often-is-it-right-mlb-2026-checked-against-2bla)

Pre-registration: `PREREG.md`, frozen by the merge of #49 (master
`8d6507f`) before this run. Run 2026-10-01 on HF `yasumorishima/mlb-stats`
revision `e006cbe12da75aac0cb96892ac8b43b9a27e5261`, `pairs.py` md5
`f4e488d3fca90da1fbf07016bad139a7`, output `primary.json` (md5
`ad2e96b4e577cf502b92a26779daba51`). The completeness checks passed
(statsapi_batting fetched 2026-10-01 00:47:58 UTC, no unplayed 2026
game, no partial row).

## The question

Two hitters are available and one has to be chosen on last season's
numbers alone. Which number most often picks the one with the higher wOBA
the next season?

## The test: 2025 numbers, 2026 outcome

353 hitters (at least 100 plate appearances in both 2025 and 2026),
62,126 pairs.

| rule (2025) | picked right | minus wOBA | 95 % interval | reading |
|---|---|---|---|---|
| wOBA | 0.6144 | | | |
| xwOBA | 0.6514 | +0.037 | +0.013 to +0.061 | better |
| wRC+ | 0.6159 | +0.002 | −0.003 to +0.006 | indistinguishable |
| blend | 0.6536 | +0.039 | +0.019 to +0.061 | better |

By the rule written before the run, xwOBA (H1) and the blend (H3) picked
the better 2026 hitter more often than last season's wOBA, and wRC+ (H2)
cannot be told apart from it. The blend's coefficients were fitted on
2015–2024 only, so in this test it is out of sample.

## Next to the development seasons

| rule | 2015–2024 (486,143 pairs) | 2025 → 2026 (62,126 pairs) |
|---|---|---|
| wOBA | 0.638 | 0.614 |
| xwOBA | 0.658 | 0.651 |
| wRC+ | 0.637 | 0.616 |
| blend | 0.661 (in-sample) | 0.654 |

Development is t = 2015–2024 without t = 2019 and t = 2020 (outcomes up
to 2025). The wOBA rule did 2.35 points worse in 2026 than in
development; xwOBA lost 0.7 points, and so did the blend against its
in-sample development figure. The gap to wOBA was wider in 2026 (+3.7
and +3.9 points) than in development (+2.0 and +2.3).

## Where the choice of rule matters

Pairs where the two rules pick opposite hitters; a pair where either
rule is tied is not counted here (495 xwOBA ties, for example).

| pairs where wOBA and … disagree | pairs | wOBA right | the other rule right |
|---|---|---|---|
| xwOBA | 12,195 | 0.407 | 0.593 |
| wRC+ | 2,108 | 0.477 | 0.523 |
| blend | 10,662 | 0.386 | 0.614 |

wOBA and xwOBA point at different hitters in about a fifth of the pairs
(12,195 of 62,126). In development the xwOBA figure there was 0.556.

## What a pick is worth

Mean 2026 wOBA of the hitter picked minus the one not picked: wOBA rule
0.0141, xwOBA 0.0193, wRC+ 0.0143, blend 0.0195.

## By plate appearances (reported, not tested)

Bands by the smaller of the two hitters' 2025 PA. Reported only, as the
pre-registration says; no reading is drawn from them.

| 2025 PA | pairs | wOBA | xwOBA | wRC+ | blend |
|---|---|---|---|---|---|
| 100–299 | 30,248 | 0.587 | 0.631 | 0.586 | 0.632 |
| 300–499 | 22,008 | 0.650 | 0.681 | 0.653 | 0.688 |
| 500+ | 9,870 | 0.620 | 0.646 | 0.623 | 0.644 |

In the 500+ band the xwOBA and blend intervals include 0 (−0.018 to
+0.072, −0.018 to +0.068); in development the 500+ band was also the one
where xwOBA's interval included 0.

## Sensitivity: a 50-PA bar in 2026

71,629 pairs: wOBA 0.614, xwOBA 0.649, wRC+ 0.616, blend 0.652. Same
order as the main test.

## Material for the decision

- Last season's wOBA picked the hitter with the higher wOBA the next
  season 61 % of the time in 2026 and 64 % in the development seasons
  (2015–2024 without 2019 and 2020).
- xwOBA picked right 65 % of the time in 2026 and 66 % in development, and
  in 2026 the difference from wOBA held up on one season alone. Where the
  two point at different hitters, xwOBA was right 59 % of the time in
  2026.
- The blend (mostly xwOBA, plus small K%, BB% and ISO terms) did about
  as well as xwOBA alone (0.654 against 0.651; that difference was not
  tested).
- wRC+, which is defined to adjust for park and league, picked right
  about as often as raw wOBA against a raw-wOBA outcome.
- The pitcher version of the same question (`../decision_pairs`) could
  not separate FIP, xERA or K−BB% from ERA on 2026 alone.
- Not covered: defence, base running, position, cost, contract, health,
  and anything scouts see that these numbers do not. Only hitters who
  reached 100 PA the next season are in the decision set.
