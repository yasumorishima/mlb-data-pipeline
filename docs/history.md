# History

README から移した過去の記録（本文は移す前のまま）。現在の構成は [README](../README.md)。

## Status (2026-06-10)

> **Status (2026-06-10):** 実行基盤は **GitHub Actions（ubuntu-latest, 週次）**、データ正本は **Hugging Face Dataset [yasumorishima/mlb-stats](https://huggingface.co/datasets/yasumorishima/mlb-stats)**。BigQuery は 2026-04-19 退役、RPi5 SSD は 2026-05-29 廃止（経緯は Migration History 参照）。

## FanGraphs の遮断と `park_factors` の移行（2026-09）

> 🔴 **遮断は 2 層あり、runner に効いているのはアドレスの方**＝pybaseball は User-Agent を一切設定せず
> 正直な既定値を送るのに、run `35565978836`（2026-09-21）で 12 シーズン全部 403。
> 一方**ブラウザを詐称した UA はどこからでも** 403 + `cf-mitigated: challenge` を返し、
> `python-requests` / `curl` / UA 無しなら住宅回線から **200**（2026-09-23 実測）。
> ⇒ 旧記述の「datacenter IP」は runner については正しく、UA の層とは別物。
`park_factors` は **2026-09-23 に Baseball Savant へ移して週次更新に復帰**
> （savant-extras 0.5.0）。それ以前は「Savant 由来」と書きながら実体が FanGraphs Guts! で、
> **HF に一度も存在しなかった**（週次ジョブは緑のまま）＝復帰ではなく**新規投入**。

## Migration History

- **Phase 1** (2026-03-25): `statcast_pitches` を `mlb_wp` → `mlb_shared` に移行
- **Phase 2** (2026-03-26): FG stats / fielding / park_factors を `mlb_shared` に統合。mlb-win-probability の独自 fetch スクリプト削除。`mlb_statcast` データセット削除
- **Phase 3** (2026-04-17): 実行基盤を RPi5 + Parquet に移行。`MLB_DATA_TARGET` env + `run_backfill.py` 追加。17/17 work units backfill 完走
- **BQ 退役** (2026-04-19): Parquet vs BQ reconcile 後、BigQuery `mlb_shared` データセット削除
- **HF 移行** (2026-05-27〜29): RPi5 USB SSD 故障・廃止に伴い、全データを public HF Dataset [yasumorishima/mlb-stats](https://huggingface.co/datasets/yasumorishima/mlb-stats) へ移行（HF が正本）
- **Phase 4** (2026-06-10): 週次 refresh を GitHub Actions（ubuntu-latest, 無料）+ HF upload に再構築、cron 再開
- **Phase 5** (2026-09-23): 出力監査（`check_outputs.py`）で通ったテーブルだけ upload・`park_factors` を Savant へ（初投入）・`statsapi_batting` / `statsapi_pitching` を新設（FanGraphs 遮断の代替）・end_year 自動化・HF の季節を失う upload を拒否
- **BigQuery sandbox** (2026-09-26): dbt マートを課金アカウントの無い GCP プロジェクトでも build（HF が正本のまま。BigQuery は同じマートの 2 つ目のエンジンで、2026-04 に退役した課金ありの保存先とは別物）。鍵を置かず Workload Identity Federation で master からだけ書き込む
- **BI 用の表** (2026-09-27): `models/bi/` に打者・投手・球種の 3 表と球団 seed を追加し、Looker Studio のダッシュボードが BigQuery sandbox から直接読む形にした（表は DuckDB と BigQuery で全セル一致、画面の件数と SQL の件数も一致）。CI はマートを Hugging Face に公開した後に bi を build するので、ダッシュボード側の失敗がマートの公開を止めない
- **Tableau Public 版** (2026-09-27): 同じ bi 表 3 本を CSV に書き出して Tableau Public に同じ 3 シート（打者・投手・球種、シーズンは単一選択・既定 2025）を作った。Tableau Public の Web 作成はファイルのアップロード（と一部のコネクタ）でしかデータを読めず BigQuery には直接つながらないので、こちらは書き出した時点のスナップショット。行数と各シートの合計は CSV から計算した値と一致。2026-09-28 に 3 シートを 1 枚のダッシュボード（Overview）にまとめ、シーズンの絞り込みを 3 つのデータソースに共通の 1 つへ統合した（2024 に切り替えて 3 表とも追従・2025 の行数と合計は変わらないことを確認）
- **指標が翌年に残る度合いのページ** (2026-10-06): 4 つの信頼性マート（打者・投手・球種・守備と走力）を 1 本の表 `bi_metric_carryover` にまとめ、Looker Studio に Carryover ページ（分野を 1 つ選び、サンプル数の帯ごと〈守備はポジションごと〉に翌年との相関を線で並べ、組数と来季 wOBA/ERA との相関も表で出す）、Tableau Public に同じ線グラフのシートを足した（Tableau は CSV スナップショット）。投球・打撃・守備・走力で画面の値がマートと一致することを確認

### Orchestrated backfill（legacy, RPi5 時代の仕組み）

`scripts/run_backfill.py` は 1 invocation = 1 work unit の orchestrator（進捗は `<MLB_PARQUET_ROOT>/.state.json`）。RPi5 systemd timer 運用（`deploy/systemd/`）は SSD 廃止に伴い停止済みだが、スクリプト自体はローカル一括 backfill に再利用可能。
