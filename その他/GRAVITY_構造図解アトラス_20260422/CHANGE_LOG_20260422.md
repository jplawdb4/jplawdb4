# CHANGE LOG

## 2026-04-22 21:36 JST

- created atlas folder:
  - `/mnt/d/Users/PC/Desktop/その他/GRAVITY_構造図解アトラス_20260422`
- created prompt library folder:
  - `/mnt/d/Users/PC/Desktop/その他/GRAVITY_構造図解アトラス_20260422/prompts`

## 2026-04-22 21:43 JST

- generated and copied 10 atlas images into the folder
- image files:
  - `gravity_atlas_01_overview_20260422.png`
  - `gravity_atlas_02_upstream_family_20260422.png`
  - `gravity_atlas_03_investment_line_20260422.png`
  - `gravity_atlas_04_gravity_essence_20260422.png`
  - `gravity_atlas_05_downstream_tmk_assets_20260422.png`
  - `gravity_atlas_06_inflow_outflow_20260422.png`
  - `gravity_atlas_07_tax_layers_20260422.png`
  - `gravity_atlas_08_withholding_treaty_20260422.png`
  - `gravity_atlas_09_evidence_map_20260422.png`
  - `gravity_atlas_10_frontier_20260422.png`
- note:
  - original generated images under `/home/user/.codex/generated_images/019dab89-dc12-7f91-8d4e-7b1ba33c8232/` were preserved

## 2026-04-22 21:45 JST

- added prompt library:
  - `prompts/gravity_visual_atlas_prompt_library_20260422.md`

## 2026-04-22 21:49 JST

- added atlas landing page:
  - `index.html`
- registered atlas in parent index:
  - `/mnt/d/Users/PC/Desktop/その他/index.html`

## 2026-04-22 22:17 JST

- updated atlas HTML layout:
  - reduced figure display width to about 80 percent on desktop
- added detailed commentary blocks:
  - 10 commentary sections, one for each figure

## 2026-04-22 22:27 JST

- added HQ prompt library:
  - `prompts/gravity_visual_atlas_hq_prompt_library_20260422.md`
- generated and copied 10 HQ replacement images into the atlas folder:
  - `gravity_atlas_01_overview_hq_20260422.png`
  - `gravity_atlas_02_upstream_family_hq_20260422.png`
  - `gravity_atlas_03_investment_line_hq_20260422.png`
  - `gravity_atlas_04_gravity_essence_hq_20260422.png`
  - `gravity_atlas_05_downstream_tmk_assets_hq_20260422.png`
  - `gravity_atlas_06_inflow_outflow_hq_20260422.png`
  - `gravity_atlas_07_tax_layers_hq_20260422.png`
  - `gravity_atlas_08_withholding_treaty_hq_20260422.png`
  - `gravity_atlas_09_evidence_map_hq_20260422.png`
  - `gravity_atlas_10_frontier_hq_20260422.png`
- switched `index.html` image references from the original 10 atlas images to the HQ set
- preserved original generated images under:
  - `/home/user/.codex/generated_images/019dab89-dc12-7f91-8d4e-7b1ba33c8232/`

## 2026-04-22 22:53 JST

- normalized all 10 HQ atlas images onto a uniform 1920x1080 canvas to remove visible size variance across sections
- added files:
  - `gravity_atlas_01_overview_hq_uniform_20260422.png`
  - `gravity_atlas_02_upstream_family_hq_uniform_20260422.png`
  - `gravity_atlas_03_investment_line_hq_uniform_20260422.png`
  - `gravity_atlas_04_gravity_essence_hq_uniform_20260422.png`
  - `gravity_atlas_05_downstream_tmk_assets_hq_uniform_20260422.png`
  - `gravity_atlas_06_inflow_outflow_hq_uniform_20260422.png`
  - `gravity_atlas_07_tax_layers_hq_uniform_20260422.png`
  - `gravity_atlas_08_withholding_treaty_hq_uniform_20260422.png`
  - `gravity_atlas_09_evidence_map_hq_uniform_20260422.png`
  - `gravity_atlas_10_frontier_hq_uniform_20260422.png`
- updated `index.html` to reference the uniform-canvas files instead of the raw HQ files
- tightened image display rules so future atlas renders stay on a 16:9 frame with `object-fit: contain`

## 2026-04-22 22:59 JST

- re-normalized the 10 `*_hq_uniform_20260422.png` atlas images so the internal content width is fixed at 1600px across all figures
- kept the outer canvas unchanged at 1920x1080
- left `index.html` references unchanged because the uniform filenames were preserved
- purpose:
  - reduce the remaining visual inconsistency where some figures looked narrower than others even after the first uniform-canvas pass

## 2026-04-22 23:03 JST

- revised the atlas hero summary and Figure 01 intro text to reduce overstatement and better separate:
  - confirmed facts
  - high-confidence inferences
  - unresolved frontier points
- tightened wording around:
  - the main line involving Japan DH4 / JIF Japan Partners LP / Gravity合同会社 / Gravity特定目的会社
  - the role of Gravity合同会社
  - the withholding / treaty claimant issue

## 2026-04-22 23:12 JST

- added a new editorial gralec prompt pack:
  - `prompts/gravity_visual_atlas_editorial_grarec_prompt_library_20260422.md`
- regenerated all 10 atlas figures in a unified commercial-magazine-grade explanatory style
- copied the newly generated files into the atlas folder as:
  - `gravity_atlas_01_overview_editorial_20260422.png`
  - `gravity_atlas_02_upstream_family_editorial_20260422.png`
  - `gravity_atlas_03_investment_line_editorial_20260422.png`
  - `gravity_atlas_04_gravity_essence_editorial_20260422.png`
  - `gravity_atlas_05_downstream_tmk_assets_editorial_20260422.png`
  - `gravity_atlas_06_inflow_outflow_editorial_20260422.png`
  - `gravity_atlas_07_tax_layers_editorial_20260422.png`
  - `gravity_atlas_08_withholding_treaty_editorial_20260422.png`
  - `gravity_atlas_09_evidence_map_editorial_20260422.png`
  - `gravity_atlas_10_frontier_editorial_20260422.png`
- created normalized atlas-delivery versions on a fixed 1920x1080 canvas with unified internal width:
  - `gravity_atlas_01_overview_editorial_uniform_20260422.png`
  - `gravity_atlas_02_upstream_family_editorial_uniform_20260422.png`
  - `gravity_atlas_03_investment_line_editorial_uniform_20260422.png`
  - `gravity_atlas_04_gravity_essence_editorial_uniform_20260422.png`
  - `gravity_atlas_05_downstream_tmk_assets_editorial_uniform_20260422.png`
  - `gravity_atlas_06_inflow_outflow_editorial_uniform_20260422.png`
  - `gravity_atlas_07_tax_layers_editorial_uniform_20260422.png`
  - `gravity_atlas_08_withholding_treaty_editorial_uniform_20260422.png`
  - `gravity_atlas_09_evidence_map_editorial_uniform_20260422.png`
  - `gravity_atlas_10_frontier_editorial_uniform_20260422.png`
- switched `index.html` references from the prior HQ uniform set to the new editorial uniform set

## 2026-04-22 23:40 JST

- reverted the atlas display from the `editorial_uniform` image set back to the previous `hq_uniform` image set
- kept the editorial files in the folder for possible later reuse
- updated only `index.html` references; no image files were deleted

## 2026-04-22 23:47 JST

- unified the atlas page onto a single content-width rail to remove the uncomfortable horizontal drift between:
  - hero / summary / toc / source box
  - figure / reading grid / commentary
- changed the shell max width from the wider mixed-layout setting to a 1120px-content-equivalent rail
- changed `.figure` from `width: min(80%, 1120px)` to `width: 100%; max-width: 1120px`
- kept the mobile fallback intact

## 2026-04-22 23:51 JST

- strengthened the width fix by putting all major layout blocks onto the exact same 1120px rail:
  - `.hero`
  - `.summary-grid`
  - `.toc`
  - `.source-box`
  - `.atlas-section`
  - `.footer-note`
- changed `.shell` from a centered max-width container to a full-width page padding wrapper so block-level alignment is explicit

## 2026-04-22 23:59 JST

- added click-to-expand image viewing to the atlas HTML
- implemented a lightbox with:
  - image click to open
  - Enter / Space keyboard open
  - overlay click to close
  - close button
  - Escape key close
- added `cursor: zoom-in` and accessibility labels for atlas figures

## 2026-04-23 00:03 JST

- fixed the top-of-page alignment issue around:
  - the 01-10 TOC block
  - the `ベースにした主要ローカル資産` block
- aligned their internal left edge by:
  - matching TOC horizontal padding to the source box
  - turning TOC links into block rows
  - removing the default `ul` indent from the source list
  - making source links block-level with clean wrapping

## 2026-04-23 00:08 JST

- narrowed the two top text-heavy blocks that still looked too left-heavy:
  - `.toc`
  - `.source-box`
- both now sit on a slimmer 920px rail inside the page so the 01-10 list and the local-assets list read closer to the visual center

## 2026-04-23 00:07 JST

- fixed the actual centering bug for the two 920px top blocks
- root cause:
  - the narrowed width had been added
  - but `margin: 30px 0 24px` and `margin: 0 0 24px` still pinned those blocks to the left
- changed to:
  - `.toc { margin: 30px auto 24px; }`
  - `.source-box { margin: 0 auto 24px; }`

## 2026-04-23 00:22 JST

- embedded per-figure primary-source hyperlinks directly into the commentary area for all 10 atlas figures
- normalized the source-link policy to prefer local audited official/raw assets under:
  - `../../tax/gravity_tk_tmk_project/20_一次資料/official/...`
- strengthened the source bundles for the most tax-sensitive figures:
  - Figure 03 now points to `FSA tokurei 012`, `FSA QII 01_b`, and Singapore open data for `JIF Japan Partners LP`
  - Figure 07 now includes `法人税法`, `相続税法`, and the NTA TMK filing packet in addition to GK/TK/TMK/shadan sources
  - Figure 08 now includes `NTA 論叢52-04` and the `MOF 日シンガポール条約 MLI概要`
  - Figure 10 now includes both `NTA 2888` and `NTA 2889`

## 2026-04-23 00:33 JST

- packaged the atlas for public delivery inside `jplawdb4/その他/GRAVITY_構造図解アトラス_20260422/`
- copied all linked primary-source assets into repo-local paths so the published atlas remains self-contained:
  - `sources/official/...`
  - `reports/...`
- rewired `index.html` so all former D-drive workspace references now resolve inside the published atlas package
- added `jplawdb4/その他/index.html` and `jplawdb4/その他/index.json` as a lightweight public landing/index for the atlas
