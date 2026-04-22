# CHANGE LOG

## 2026-04-22

- created:
  - `index.html`
- added image asset:
  - `gravity_group_structure_infographic_20260422.png`
- purpose:
  - `Gravity合同会社`
  - `JIF Japan Partners LP`
  - `Japan DH4 投資事業有限責任組合`
  - `Gravity特定目的会社`
  を中心に、GRAVITY グループの current best structure を
  `確定 / 高確度推定 / 未確定`
  に分けて図解する納品版 HTML を作成
- diagram coverage:
  - layered structure
  - legal / economic / tax flow
  - player map
  - edge-by-edge audit
  - tax and treaty implications
  - frontier and source map
- image coverage:
  - 上記HTMLの構造を1枚に圧縮したインフォグラフィック PNG を追加
  - 納品HTML内に画像プレビューと直リンクを追加
- based on:
  - `/mnt/d/Users/PC/Desktop/tax/gravity_tk_tmk_project/90_成果/gravity_group_structure_deep_research_20260422.md`
  - `/mnt/d/Users/PC/Desktop/tax/gravity_tk_tmk_project/90_成果/gravity_current_status_deep_research_20260422.md`
  - `/mnt/d/Users/PC/Desktop/tax/gravity_tk_tmk_project/30_分析/ストラクチャー/JIF_LP_直上直下候補マトリクス_20260422.md`
  - `/mnt/d/Users/PC/Desktop/tax/gravity_tk_tmk_project/30_分析/ストラクチャー/Gravity下流TMK候補_20260422.md`
  - `/mnt/d/Users/PC/Desktop/tax/gravity_tk_tmk_project/30_分析/ストラクチャー/GRAVITY_GK_TK_TMK_一般社団法人_税務レイヤー_20260422.md`
  - `/mnt/d/Users/PC/Desktop/tax/gravity_tk_tmk_project/90_成果/gravity_withholding_tax_and_treaty_benefit_deep_research_20260422.md`

## 2026-04-22 image refresh

- added image asset:
  - `gravity_group_structure_infographic_20260422_v2.png`
- updated:
  - `index.html`
- note:
  - built-in `image_gen` に長文構造プロンプトを投入して再生成した最新版を `v2` として保存
  - 旧版 `gravity_group_structure_infographic_20260422.png` は比較用に残し、HTML の表示先だけ最新版へ切り替えた

## 2026-04-22 image refresh japanese-main

- added image asset:
  - `gravity_group_structure_infographic_20260422_v3_japanese.png`
- updated:
  - `index.html`
- note:
  - 日本語主体で見える版に振り直した再生成画像を `v3` として保存
  - HTML の画像表示先を `v3` に切り替えた
  - `v1` と `v2` は比較用にそのまま残した

## 2026-04-22 design refresh

- updated:
  - `index.html`
- design direction:
  - エディトリアルなデューデリジェンス資料
  - 左レール型ナビゲーション
  - セリフ見出し + サンセリフ本文のタイポグラフィ分離
  - 暖色紙面 + ネイビーアクセント
  - 情報階層を強くした sheet / dossier / bento 構成
- note:
  - 既存の内容と図版は維持したまま、見た目を「研究メモ」から「洗練された納品HTML」へ全面刷新
  - 生成図解画像、層構造図、フロー図、エッジ監査、税務読解、ローカル成果物、一次ソースの導線はそのまま保持

## 2026-04-22 non-card editorial redesign

- updated:
  - `index.html`
- design direction:
  - カード列の反復を廃止
  - 左レール目次 + 本文紙面 + 罫線ベースの編集レイアウト
  - セクション見出し、台帳行、比較列、論点列で読む構造に再編
- note:
  - 「ハイセンス化」だけでなく、投資・税務ストラクチャー資料として読みやすい紙面へ抜本的に再設計
  - 角丸カードの入れ子感を避け、長文でも視線が流れる editorial dossier 形式に寄せた

## 2026-04-22 section-image redesign

- added image assets:
  - `gravity_section_01_overview.svg`
  - `gravity_section_02_evidence_map.svg`
  - `gravity_section_03_layered_structure.svg`
  - `gravity_section_04_three_flows.svg`
  - `gravity_section_05_player_roles.svg`
  - `gravity_section_06_tax_reading.svg`
  - `gravity_section_07_comparison_frontier.svg`
- updated:
  - `index.html`
- design direction:
  - 各論点を「1セクション = 1図」の image-led 構成へ再編
  - 図の直下に短い解説を置き、本文は補助線に回す
- note:
  - 既存の長文HTMLを読みやすくするため、章立てを 7 枚の図版セットに再構成した
  - 元の1枚インフォグラフィックは参照導線として残しつつ、本文ではセクション単位の SVG 図版を主役にした

## 2026-04-22 ai-generated poster integration

- added image asset:
  - `gravity_group_structure_ai_sections_poster_20260422.png`
- updated:
  - `index.html`
- note:
  - 画像生成モデルで作成した日本語主体の 7 セクション統合ポスターを納品フォルダへ保存した
  - HTML の先頭に master visual として組み込み、既存 SVG 群は補助のセクション図版として残した

## 2026-04-22 ten-image atlas html

- added image assets:
  - `gravity_ai_visual_01_overview_20260422.png`
  - `gravity_ai_visual_02_evidence_map_20260422.png`
  - `gravity_ai_visual_03_layered_structure_20260422.png`
  - `gravity_ai_visual_04_domestic_gk_tk_tmk_20260422.png`
  - `gravity_ai_visual_05_three_flows_20260422.png`
  - `gravity_ai_visual_06_player_taxonomy_20260422.png`
  - `gravity_ai_visual_07_tax_reading_20260422.png`
  - `gravity_ai_visual_08_foreign_sleeve_20260422.png`
  - `gravity_ai_visual_09_comparison_frontier_20260422.png`
  - `gravity_ai_visual_10_executive_summary_20260422.png`
- added html:
  - `ai_generated_visual_atlas_20260422.html`
- updated:
  - `index.html`
- note:
  - 画像生成モデルに full-text 前提で投げた 10 枚を、順序付きで読む補充編 HTML に束ねた
  - 主報告は残しつつ、画像主導で理解したいときの companion atlas として追加した

## 2026-04-22 atlas redesign

- updated:
  - `ai_generated_visual_atlas_20260422.html`
- design direction:
  - 一覧ページから、Apple 風の静かな高級感を持つ editorial gallery へ再設計
  - 巨大タイポ、広い余白、薄いガラス感、抑制したカラーで読む構成へ変更
- note:
  - 無難な paper layout をやめ、Apple の発表ページのような静かな product-storytelling 方向へ振り切った
  - 画像は同じでも、読み順と視線誘導が明確になるようにレイアウトを再設計した

## 2026-04-22 mobile typography optimization

- updated:
  - `ai_generated_visual_atlas_20260422.html`
- design direction:
  - スマホでの見出し圧を下げ、Apple 風の静かな階層感を維持したまま文字サイズを最適化
- note:
  - hero 見出し、章タイトル、巨大番号、chip、summary band、余白を全体的に縮小
  - `@media (max-width: 760px)` でモバイル専用のタイポグラフィと spacing を追加した
